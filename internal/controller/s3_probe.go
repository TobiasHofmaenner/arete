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
	logf "sigs.k8s.io/controller-runtime/pkg/log"

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
//
// Two phases:
//  1. Cheap reachability LIST under spec.prefix (caps at probeReachabilityCap).
//  2. Format-aware sentinel hunt for detectedFormat/detectedVersion.
//
// Phase 2 is best-effort; failure to find a sentinel does NOT flip Reachable.
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

	// Phase 1: reachability. Non-recursive LIST is enough — we only need to
	// confirm creds + auth + that the prefix is queryable. Empty prefix is
	// still Reachable=True (Healthy will reject it in Pass 3).
	const probeReachabilityCap = 10
	listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	count := 0
	for obj := range client.ListObjects(listCtx, spec.S3.Bucket, minio.ListObjectsOptions{
		Prefix:    spec.S3.Prefix + "/",
		Recursive: false,
	}) {
		if obj.Err != nil {
			reason, msg := classifyS3Error(obj.Err)
			return probeResult{Reachable: false, Reason: reason, Message: msg}
		}
		count++
		if count >= probeReachabilityCap {
			break
		}
	}

	result := probeResult{
		Reachable: true,
		Reason:    "ProbeSucceeded",
		Message:   fmt.Sprintf("prefix reachable, %d top-level entries sampled", count),
	}

	// Phase 2: format-aware sentinel hunt.
	format, version := detectFormatVersion(ctx, client, spec)
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

// detectFormatVersion does a format-specific targeted lookup for the
// producer version. Best-effort; returns ("", "") if the sentinel is
// missing or unparseable.
func detectFormatVersion(
	ctx context.Context, client *minio.Client, spec aretev1alpha1.BackupRepositorySpec,
) (string, string) {
	switch spec.Format {
	case aretev1alpha1.BackupFormatWalg:
		return "walg", detectWalgVersion(ctx, client, spec)
	case aretev1alpha1.BackupFormatRestic:
		return "restic", detectResticVersion(ctx, client, spec)
	case aretev1alpha1.BackupFormatBarman:
		// Barman sentinel parsing not implemented in Pass 2 — format is
		// reported but version stays empty.
		return "barman", ""
	}
	return "", ""
}

// --- wal-g ---

// walgSentinel is the subset of fields we read from
// basebackups_005/<TS>_backup_stop_sentinel.json.
//
// NOTE: wal-g does NOT write its binary version into the sentinel. The
// `Version` field is the sentinel JSON FORMAT version (an int). The
// closest forward-compat signal we can surface from L1 is the
// (sentinel-format, postgres-version) tuple — actual wal-g binary
// compatibility is enforced by Layer-2 (where arete's pinned validator
// either parses the repo or doesn't).
type walgSentinel struct {
	Version   int    `json:"Version"`   // sentinel format version (e.g. 2)
	PgVersion int    `json:"PgVersion"` // packed: 160008 = "16.0.8"
	Hostname  string `json:"Hostname"`
}

func detectWalgVersion(
	ctx context.Context, client *minio.Client, spec aretev1alpha1.BackupRepositorySpec,
) string {
	log := logf.FromContext(ctx)
	const suffix = "_backup_stop_sentinel.json"

	// wal-g writes one sentinel per basebackup, all under
	// `<prefix>/basebackups_005/`. List that subdir non-recursively to skip
	// the per-backup tar_partitions noise — sentinels live at the top of
	// the basebackups dir alongside the per-backup subdirs.
	listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	listPrefix := spec.S3.Prefix + "/basebackups_005/"
	log.Info("walg sentinel hunt starting", "bucket", spec.S3.Bucket, "prefix", listPrefix)

	var sentinels []minio.ObjectInfo
	total := 0
	for obj := range client.ListObjects(listCtx, spec.S3.Bucket, minio.ListObjectsOptions{
		Prefix:    listPrefix,
		Recursive: false,
	}) {
		if obj.Err != nil {
			log.Error(obj.Err, "walg sentinel hunt list error")
			return ""
		}
		total++
		if strings.HasSuffix(obj.Key, suffix) {
			sentinels = append(sentinels, obj)
		}
	}
	log.Info("walg sentinel hunt finished", "totalObjects", total, "sentinelsFound", len(sentinels))
	if len(sentinels) == 0 {
		return ""
	}
	// Pick the most recent sentinel — reflects the version that wrote the
	// latest backup, which is what we want to compare to arete's validator.
	sort.Slice(sentinels, func(i, j int) bool {
		return sentinels[i].LastModified.After(sentinels[j].LastModified)
	})

	body, err := getObjectBody(ctx, client, spec.S3.Bucket, sentinels[0].Key)
	if err != nil {
		log.Error(err, "walg sentinel GET failed", "key", sentinels[0].Key)
		return ""
	}
	var s walgSentinel
	if err := json.Unmarshal(body, &s); err != nil {
		log.Error(err, "walg sentinel JSON parse failed", "key", sentinels[0].Key)
		return ""
	}
	return fmt.Sprintf("sentinel-v%d/pg-%s", s.Version, formatPgVersion(s.PgVersion))
}

// formatPgVersion turns wal-g's packed PgVersion (160008) into "16.0.8".
// Postgres encoding: major*10000 + minor*100 + patch (pre-10) or
// major*10000 + patch (10+). For 10+, the middle digit is always 0.
func formatPgVersion(v int) string {
	if v == 0 {
		return "unknown"
	}
	major := v / 10000
	patch := v % 100
	return fmt.Sprintf("%d.0.%d", major, patch)
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
	ctx context.Context, client *minio.Client, spec aretev1alpha1.BackupRepositorySpec,
) string {
	// restic config lives at a fixed key relative to the repo prefix.
	// GET it directly — no need to list first.
	body, err := getObjectBody(ctx, client, spec.S3.Bucket, spec.S3.Prefix+"/config")
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
