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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// probeResult is the structured outcome of one E1 cycle. Each block maps
// 1:1 to an E1 sub-condition; the controller composes them into rollups.
type probeResult struct {
	// Reachable (E1 sub) — bucket+prefix accessible via LIST
	Reachable        bool
	ReachableReason  string
	ReachableMessage string

	// BucketSecurityValid (E1 sub) — declared bucket security posture
	// matches what the backend reports
	BucketSecurityValid   bool
	BucketSecurityReason  string
	BucketSecurityMessage string

	// Producer-claimed metadata. Only the load-bearing fields E1 can
	// populate cheaply: the most recent backup's timestamp (drives the
	// BackupCurrent condition) and detected format/version strings.
	// Rich per-backup detail (size, paths, hostname) lives in
	// claimedLatestBackup, populated by E2 at the slower validation
	// cadence — keeping recency consistent within each substruct.
	DetectedFormat       string
	DetectedVersion      string
	LastSuccessfulBackup *metav1.Time
}

// probeRepository runs one E1 cycle. Pure function over (spec, creds) — no
// Kubernetes side effects. The returned probeResult captures every E1 sub-
// signal the controller needs to set conditions; downstream computations
// (BackupCurrent, ProbeHealthy rollup) live in the controller.
func probeRepository(
	ctx context.Context, spec aretev1alpha1.BackupRepositorySpec, creds S3Credentials,
) probeResult {
	client, err := buildS3Client(spec.S3, creds)
	if err != nil {
		return probeResult{
			Reachable:        false,
			ReachableReason:  aretev1alpha1.ReasonClientBuildFailed,
			ReachableMessage: err.Error(),
		}
	}

	// Phase 1: reachability via cheap non-recursive LIST.
	reachable, rReason, rMsg := checkReachability(ctx, client, spec)
	result := probeResult{
		Reachable:        reachable,
		ReachableReason:  rReason,
		ReachableMessage: rMsg,
	}
	if !reachable {
		// No point checking security or sentinels if we can't list.
		return result
	}

	// Phase 2: bucket security posture (independent of phase 1 success).
	secValid, secReason, secMsg := checkBucketSecurity(ctx, client, spec)
	result.BucketSecurityValid = secValid
	result.BucketSecurityReason = secReason
	result.BucketSecurityMessage = secMsg

	// Phase 3: format-aware sentinel parse (extracts version + last
	// successful backup timestamp; rich detail is E2's job).
	format, version, lastSuccess := detectFormatAndTimestamp(ctx, client, spec)
	result.DetectedFormat = format
	result.DetectedVersion = version
	result.LastSuccessfulBackup = lastSuccess

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

// checkReachability runs a bounded non-recursive LIST under the prefix to
// confirm creds + auth + that the prefix is queryable. Empty prefix is
// still Reachable=True (BackupCurrent will reject it via lag check).
func checkReachability(
	ctx context.Context, client *minio.Client, spec aretev1alpha1.BackupRepositorySpec,
) (bool, string, string) {
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
			return false, reason, msg
		}
		count++
		if count >= probeReachabilityCap {
			break
		}
	}
	return true, aretev1alpha1.ReasonProbeSucceeded,
		fmt.Sprintf("prefix reachable, %d top-level entries sampled", count)
}

// checkBucketSecurity verifies the declared bucket security posture matches
// what the backend reports. Today's checks:
//   - HTTPS endpoint (admission rejects http://, but re-verify defensively)
//   - Object lock (only if spec.s3.requireObjectLock == true)
//   - Bucket encryption (only if spec.s3.requireBucketEncryption == true)
//
// Public-access-block parsing is deferred (requires bucket policy
// inspection — not all S3-compat backends expose it consistently).
func checkBucketSecurity(
	ctx context.Context, client *minio.Client, spec aretev1alpha1.BackupRepositorySpec,
) (bool, string, string) {
	if !strings.HasPrefix(spec.S3.Endpoint, "https://") {
		return false, aretev1alpha1.ReasonInsecureEndpoint,
			"endpoint must use https://"
	}

	checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	var checked []string

	if spec.S3.RequireObjectLock {
		_, _, _, _, err := client.GetObjectLockConfig(checkCtx, spec.S3.Bucket)
		if err != nil {
			return false, aretev1alpha1.ReasonObjectLockMissing,
				fmt.Sprintf("requireObjectLock=true but bucket reports: %s", err.Error())
		}
		checked = append(checked, "object-lock")
	}

	if spec.S3.RequireBucketEncryption {
		_, err := client.GetBucketEncryption(checkCtx, spec.S3.Bucket)
		if err != nil {
			return false, aretev1alpha1.ReasonBucketEncryptionMissing,
				fmt.Sprintf("requireBucketEncryption=true but bucket reports: %s", err.Error())
		}
		checked = append(checked, "bucket-encryption")
	}

	msg := "https endpoint"
	if len(checked) > 0 {
		msg = fmt.Sprintf("https endpoint, verified: %s", strings.Join(checked, ", "))
	}
	return true, aretev1alpha1.ReasonProbeSucceeded, msg
}

// classifyS3Error turns a raw minio-go error into a (reason, message) pair
// suitable for a Reachable=False condition. CamelCase reason → alert label.
func classifyS3Error(err error) (string, string) {
	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
		switch resp.Code {
		case "NoSuchBucket":
			return aretev1alpha1.ReasonBucketNotFound, resp.Message
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch":
			return aretev1alpha1.ReasonCredentialsRejected, resp.Message
		default:
			if resp.Code != "" {
				return aretev1alpha1.ReasonS3APIError,
					fmt.Sprintf("%s: %s", resp.Code, resp.Message)
			}
		}
	}
	return aretev1alpha1.ReasonS3Unreachable, err.Error()
}

// detectFormatAndTimestamp does a format-specific lookup for the producer
// version + the most recent successful backup's timestamp (drives the
// BackupCurrent condition). Per-backup detail (size/paths) is E2's job
// — populated at the slower validation cadence so claimedLatestBackup's
// recency stays consistent across all its fields.
func detectFormatAndTimestamp(
	ctx context.Context, client *minio.Client, spec aretev1alpha1.BackupRepositorySpec,
) (format string, version string, lastSuccess *metav1.Time) {
	switch spec.Format {
	case aretev1alpha1.BackupFormatWalg:
		v, t := readWalgSentinelTimestamp(ctx, client, spec)
		return "walg", v, t
	case aretev1alpha1.BackupFormatRestic:
		// detectedVersion stays empty for restic at E1 — restic encrypts
		// the config file, so version determination requires E2.
		return "restic", "", readResticLatestSnapshotTime(ctx, client, spec)
	case aretev1alpha1.BackupFormatBarman:
		// Barman sentinel parsing not implemented yet.
		return "barman", "", nil
	}
	return "", "", nil
}

// --- wal-g ---

// walgSentinel is the subset of fields we read from
// basebackups_005/<TS>_backup_stop_sentinel.json at E1.
//
// NOTE: wal-g does NOT write its binary version into the sentinel. The
// `Version` field is the sentinel JSON FORMAT version (an int). The
// closest forward-compat signal we can surface from E1 is the
// (sentinel-format, postgres-version) tuple — actual wal-g binary
// compatibility is enforced by E2 (where arete's pinned validator either
// parses the repo or doesn't). Per-backup size + name now come from E2
// (`wal-g backup-list --detail --json`) so they live in claimedLatestBackup
// alongside the same fields populated for restic, with consistent recency.
type walgSentinel struct {
	Version    int    `json:"Version"`   // sentinel format version (e.g. 2)
	PgVersion  int    `json:"PgVersion"` // packed: 160008 = "16.0.8"
	FinishTime string `json:"FinishTime"`
}

// readWalgSentinelTimestamp returns the detected version string and the
// FinishTime of the most-recent basebackup sentinel — the load-bearing
// timestamp that drives the BackupCurrent condition. Per-backup detail
// (name/size) is E2's job.
func readWalgSentinelTimestamp(
	ctx context.Context, client *minio.Client, spec aretev1alpha1.BackupRepositorySpec,
) (string, *metav1.Time) {
	log := logf.FromContext(ctx)
	const suffix = "_backup_stop_sentinel.json"

	// wal-g writes one sentinel per basebackup at the top of
	// `<prefix>/basebackups_005/`. List that subdir non-recursively to skip
	// per-backup tar_partitions noise — sentinels live alongside the
	// per-backup subdirs as direct children.
	listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	listPrefix := spec.S3.Prefix + "/basebackups_005/"

	var sentinels []minio.ObjectInfo
	for obj := range client.ListObjects(listCtx, spec.S3.Bucket, minio.ListObjectsOptions{
		Prefix:    listPrefix,
		Recursive: false,
	}) {
		if obj.Err != nil {
			log.Error(obj.Err, "walg sentinel hunt list error")
			return "", nil
		}
		if strings.HasSuffix(obj.Key, suffix) {
			sentinels = append(sentinels, obj)
		}
	}
	if len(sentinels) == 0 {
		return "", nil
	}
	// Pick the most recent sentinel — reflects the version that wrote the
	// latest backup, which is what we want to compare to arete's validator.
	sort.Slice(sentinels, func(i, j int) bool {
		return sentinels[i].LastModified.After(sentinels[j].LastModified)
	})

	body, err := getObjectBody(ctx, client, spec.S3.Bucket, sentinels[0].Key)
	if err != nil {
		log.Error(err, "walg sentinel GET failed", "key", sentinels[0].Key)
		return "", nil
	}
	var s walgSentinel
	if err := json.Unmarshal(body, &s); err != nil {
		log.Error(err, "walg sentinel JSON parse failed", "key", sentinels[0].Key)
		return "", nil
	}

	version := fmt.Sprintf("sentinel-v%d/pg-%s", s.Version, formatPgVersion(s.PgVersion))
	createdAt := parseWalgTime(s.FinishTime, sentinels[0].LastModified)
	t := metav1.NewTime(createdAt)
	return version, &t
}

// parseWalgTime parses wal-g's time format (RFC3339-ish with microseconds)
// with fallback to the supplied default if parsing fails.
func parseWalgTime(raw string, fallback time.Time) time.Time {
	if raw == "" {
		return fallback
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.000000Z",
		"2006-01-02T15:04:05Z",
	} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t
		}
	}
	return fallback
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

// readResticLatestSnapshotTime enumerates <prefix>/snapshots/ and returns
// the LastModified of the most recent snapshot file — the load-bearing
// timestamp for BackupCurrent.
//
// IMPORTANT: restic encrypts EVERYTHING in the repo, including config and
// snapshot contents. We can't determine the restic format version, the
// snapshot ID's metadata, or the per-backup size at E1. Those live in
// claimedLatestBackup, populated by E2 from `restic snapshots --json`.
//
// Snapshot file LastModified is reliable — restic creates one file per
// snapshot and never rewrites them.
func readResticLatestSnapshotTime(
	ctx context.Context, client *minio.Client, spec aretev1alpha1.BackupRepositorySpec,
) *metav1.Time {
	log := logf.FromContext(ctx)

	listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var newest time.Time
	count := 0
	for obj := range client.ListObjects(listCtx, spec.S3.Bucket, minio.ListObjectsOptions{
		Prefix:    spec.S3.Prefix + "/snapshots/",
		Recursive: false,
	}) {
		if obj.Err != nil {
			log.Error(obj.Err, "restic snapshots list error")
			return nil
		}
		count++
		if obj.LastModified.After(newest) {
			newest = obj.LastModified
		}
	}
	if count == 0 {
		return nil
	}
	t := metav1.NewTime(newest)
	return &t
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
