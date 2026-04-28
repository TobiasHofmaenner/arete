/*
Copyright 2026 Tobias Hofmaenner.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	aretev1alpha1 "github.com/TobiasHofmaenner/arete/api/v1alpha1"
)

// We use minio-go (not aws-sdk-go-v2) because aws-sdk-go-v2 emits
// `Amz-Sdk-Invocation-Id` / `Amz-Sdk-Request` middleware headers that
// some proxy chains (notably Cloudflare-fronted Ceph RGW) mangle, breaking
// SigV4 verification at the backend. minio-go is purpose-built for
// S3-compatible backends and avoids those headers entirely.

// S3Credentials carries the resolved values from the credentialsSecret.
type S3Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string // optional, for STS-issued creds
}

// probeResult is the outcome of one Layer-1 cycle.
type probeResult struct {
	Reachable       bool
	Reason          string // condition Reason (CamelCase)
	Message         string // condition Message (human-readable)
	DetectedFormat  string
	DetectedVersion string
}

// probeRepository runs one Layer-1 probe against the configured repository.
// Pure function over (spec, creds) — no Kubernetes side effects.
func probeRepository(
	ctx context.Context, spec aretev1alpha1.BackupRepositorySpec, creds S3Credentials,
) probeResult {
	client, err := buildS3Client(spec.S3, creds)
	if err != nil {
		return probeResult{
			Reachable: false,
			Reason:    "ClientBuildFailed",
			Message:   err.Error(),
		}
	}

	// Bounded LIST under the prefix — confirms creds + auth + reachability
	// in one call. Capped at sampleLimit so a giant repo doesn't tax us.
	const sampleLimit = 50
	listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var sample []minio.ObjectInfo
	for obj := range client.ListObjects(listCtx, spec.S3.Bucket, minio.ListObjectsOptions{
		Prefix:    spec.S3.Prefix + "/",
		Recursive: true,
	}) {
		if obj.Err != nil {
			reason, msg := classifyS3Error(obj.Err)
			return probeResult{Reachable: false, Reason: reason, Message: msg}
		}
		sample = append(sample, obj)
		if len(sample) >= sampleLimit {
			break
		}
	}

	result := probeResult{
		Reachable: true,
		Reason:    "ProbeSucceeded",
		Message:   fmt.Sprintf("listed %d objects under prefix", len(sample)),
	}

	// Best-effort sentinel detection. Failure to find a sentinel does NOT
	// flip Reachable — the bucket+prefix were demonstrably reachable. Empty
	// or unrecognised content just leaves DetectedVersion blank for now;
	// Layer-2 will ultimately decide Healthy.
	format, version := detectFormatVersion(ctx, client, spec, sample)
	result.DetectedFormat = format
	result.DetectedVersion = version
	return result
}

func buildS3Client(src aretev1alpha1.S3Source, creds S3Credentials) (*minio.Client, error) {
	u, err := url.Parse(src.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint %q: %w", src.Endpoint, err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("endpoint %q has no host", src.Endpoint)
	}
	secure := u.Scheme == "https"
	return minio.New(u.Host, &minio.Options{
		Creds: credentials.NewStaticV4(
			creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken,
		),
		Secure: secure,
		Region: src.Region,
	})
}

// classifyS3Error turns a raw minio-go error into a (reason, message) pair
// suitable for a Reachable=False condition. CamelCase reason → alert label.
func classifyS3Error(err error) (string, string) {
	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
		switch resp.Code {
		case "NoSuchBucket":
			return "BucketNotFound", resp.Message
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch":
			return "CredentialsRejected", resp.Message
		default:
			if resp.Code != "" {
				return "S3APIError", fmt.Sprintf("%s: %s", resp.Code, resp.Message)
			}
		}
	}
	return "S3Unreachable", err.Error()
}

// detectFormatVersion inspects the listed sample for a format-specific
// sentinel matching spec.format and parses the producer version out of it.
// Best-effort; returns ("", "") if no sentinel is found or parsing fails.
func detectFormatVersion(
	ctx context.Context, client *minio.Client,
	spec aretev1alpha1.BackupRepositorySpec, sample []minio.ObjectInfo,
) (string, string) {
	switch spec.Format {
	case aretev1alpha1.BackupFormatWalg:
		return "walg", detectWalgVersion(ctx, client, spec, sample)
	case aretev1alpha1.BackupFormatRestic:
		return "restic", detectResticVersion(ctx, client, spec, sample)
	case aretev1alpha1.BackupFormatBarman:
		// Barman sentinel parsing not implemented in Pass 2 — format is
		// reported but version stays empty.
		return "barman", ""
	}
	return "", ""
}

// --- wal-g ---

// walgSentinel is the subset of fields we read from
// basebackups_005/<TS>_backup_stop_sentinel.json. Extra fields ignored.
type walgSentinel struct {
	Version string `json:"Version"`
}

func detectWalgVersion(
	ctx context.Context, client *minio.Client,
	spec aretev1alpha1.BackupRepositorySpec, sample []minio.ObjectInfo,
) string {
	const suffix = "_backup_stop_sentinel.json"

	// Collect all sentinel keys in the sample, pick the most recent by
	// LastModified. wal-g writes a new sentinel per basebackup; the latest
	// reflects the version that wrote the most recent backup — which is
	// what we want to compare against arete's pinned validator.
	var sentinels []minio.ObjectInfo
	for _, o := range sample {
		if strings.HasSuffix(o.Key, suffix) {
			sentinels = append(sentinels, o)
		}
	}
	if len(sentinels) == 0 {
		return ""
	}
	sort.Slice(sentinels, func(i, j int) bool {
		return sentinels[i].LastModified.After(sentinels[j].LastModified)
	})

	body, err := getObjectBody(ctx, client, spec.S3.Bucket, sentinels[0].Key)
	if err != nil {
		return ""
	}
	var s walgSentinel
	if err := json.Unmarshal(body, &s); err != nil {
		return ""
	}
	return s.Version
}

// --- restic ---

// resticConfig is the JSON at <prefix>/config. The "version" field is the
// REPO format version (1 or 2), not the restic binary version — but it's
// still useful as a forward-compat signal because restic 0.14+ writes v2
// repos and older binaries can't read them.
type resticConfig struct {
	Version int    `json:"version"`
	ID      string `json:"id"`
}

func detectResticVersion(
	ctx context.Context, client *minio.Client,
	spec aretev1alpha1.BackupRepositorySpec, sample []minio.ObjectInfo,
) string {
	configKey := spec.S3.Prefix + "/config"
	hasConfig := false
	for _, o := range sample {
		if o.Key == configKey {
			hasConfig = true
			break
		}
	}
	if !hasConfig {
		return ""
	}
	body, err := getObjectBody(ctx, client, spec.S3.Bucket, configKey)
	if err != nil {
		return ""
	}
	var c resticConfig
	if err := json.Unmarshal(body, &c); err != nil {
		return ""
	}
	return fmt.Sprintf("repo-v%d", c.Version)
}

// getObjectBody reads an S3 object fully into memory. Only used for tiny
// sentinel/config files — guarded by a hard size cap to refuse to slurp
// anything large by accident.
func getObjectBody(ctx context.Context, client *minio.Client, bucket, key string) ([]byte, error) {
	const maxBytes = 64 * 1024 // sentinel files are well under 1 KiB

	obj, err := client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	return io.ReadAll(io.LimitReader(obj, maxBytes))
}
