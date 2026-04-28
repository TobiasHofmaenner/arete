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
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	aretev1alpha1 "github.com/TobiasHofmaenner/arete/api/v1alpha1"
)

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
	client, err := buildS3Client(ctx, spec.S3, creds)
	if err != nil {
		return probeResult{
			Reachable: false,
			Reason:    "ClientBuildFailed",
			Message:   err.Error(),
		}
	}

	// Single LIST is enough to test reachability + auth + prefix existence.
	// MaxKeys is bounded — we just need a sample to hunt for sentinels.
	const sampleLimit = 50
	listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(spec.S3.Bucket),
		Prefix:  aws.String(spec.S3.Prefix + "/"),
		MaxKeys: aws.Int32(sampleLimit),
	})
	if err != nil {
		reason, msg := classifyS3Error(err)
		return probeResult{Reachable: false, Reason: reason, Message: msg}
	}

	result := probeResult{
		Reachable: true,
		Reason:    "ProbeSucceeded",
		Message:   fmt.Sprintf("listed %d objects under prefix", len(listOut.Contents)),
	}

	// Best-effort sentinel detection. Failure to find a sentinel does NOT
	// flip Reachable — the bucket+prefix were demonstrably reachable. Empty
	// or unrecognised content just leaves DetectedVersion blank for now;
	// Layer-2 will ultimately decide Healthy.
	format, version := detectFormatVersion(ctx, client, spec, listOut.Contents)
	result.DetectedFormat = format
	result.DetectedVersion = version
	return result
}

func buildS3Client(
	ctx context.Context, src aretev1alpha1.S3Source, creds S3Credentials,
) (*s3.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(src.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken,
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(src.Endpoint)
		// Path-style addressing — works for both AWS S3 and MinIO/Ceph RGW
		// without requiring DNS for virtual-hosted style.
		o.UsePathStyle = true
	}), nil
}

// classifyS3Error turns a raw SDK error into a (reason, message) pair
// suitable for a Reachable=False condition. CamelCase reason → alert label.
func classifyS3Error(err error) (string, string) {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		switch code {
		case "NoSuchBucket":
			return "BucketNotFound", apiErr.ErrorMessage()
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch":
			return "CredentialsRejected", apiErr.ErrorMessage()
		default:
			return "S3APIError", fmt.Sprintf("%s: %s", code, apiErr.ErrorMessage())
		}
	}
	return "S3Unreachable", err.Error()
}

// detectFormatVersion inspects the listed sample for a format-specific
// sentinel matching spec.format and parses the producer version out of it.
// Best-effort; returns ("", "") if no sentinel is found or parsing fails.
func detectFormatVersion(
	ctx context.Context, client *s3.Client,
	spec aretev1alpha1.BackupRepositorySpec, sample []s3types.Object,
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
	ctx context.Context, client *s3.Client,
	spec aretev1alpha1.BackupRepositorySpec, sample []s3types.Object,
) string {
	const suffix = "_backup_stop_sentinel.json"

	// Collect all sentinel keys in the sample, pick the most recent by
	// LastModified. wal-g writes a new sentinel per basebackup; the latest
	// reflects the version that wrote the most recent backup — which is
	// what we want to compare against arete's pinned validator.
	var sentinels []s3types.Object
	for _, o := range sample {
		if o.Key != nil && strings.HasSuffix(*o.Key, suffix) {
			sentinels = append(sentinels, o)
		}
	}
	if len(sentinels) == 0 {
		return ""
	}
	sort.Slice(sentinels, func(i, j int) bool {
		return sentinels[i].LastModified.After(*sentinels[j].LastModified)
	})

	body, err := getObjectBody(ctx, client, spec.S3.Bucket, *sentinels[0].Key)
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
	ctx context.Context, client *s3.Client,
	spec aretev1alpha1.BackupRepositorySpec, sample []s3types.Object,
) string {
	configKey := spec.S3.Prefix + "/config"
	hasConfig := false
	for _, o := range sample {
		if o.Key != nil && *o.Key == configKey {
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
func getObjectBody(ctx context.Context, client *s3.Client, bucket, key string) ([]byte, error) {
	const maxBytes = 64 * 1024 // sentinel files are well under 1 KiB

	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(io.LimitReader(out.Body, maxBytes))
}
