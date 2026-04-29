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
	"k8s.io/apimachinery/pkg/api/resource"
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

	// Producer-claimed metadata. Per-format ownership of claimedLatestBackup
	// (see ADR-023 / project_arete_pass3_status_additions memory):
	//   - wal-g: E1 sentinel parsing OWNS the full LatestBackup struct
	//     (sentinels are plaintext; size + timestamp + name all available
	//     here for free). E2 doesn't touch it.
	//   - restic: E2 `restic snapshots --json` OWNS the full struct
	//     (everything's encrypted, E1 can't see beyond LastModified).
	//     E1 only sets LastSuccessfulBackup (timestamp) for BackupCurrent.
	// LastSuccessfulBackup is set by E1 for both formats — the load-bearing
	// timestamp for BackupCurrent.
	DetectedFormat       string
	DetectedVersion      string
	LastSuccessfulBackup *metav1.Time
	LatestBackup         *aretev1alpha1.LatestBackupStatus // wal-g only at E1; nil for restic

	// Inventory is the result of the recursive LIST run alongside the
	// E1 reachability check. nil if reachability failed (no point
	// scanning a prefix we couldn't reach).
	Inventory *aretev1alpha1.InventoryStatus
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

	// Phase 3: format-aware sentinel parse. wal-g returns full latestBackup
	// (sentinel is plaintext); restic returns only the timestamp (everything
	// else needs decryption — that's E2's job).
	format, version, lastSuccess, latest := detectFormatAndTimestamp(ctx, client, spec)
	result.DetectedFormat = format
	result.DetectedVersion = version
	result.LastSuccessfulBackup = lastSuccess
	result.LatestBackup = latest

	// Phase 4: inventory. Best-effort; if it fails we leave the field
	// nil and the controller preserves the previous status.
	if inv, err := runInventoryProbe(ctx, client, spec); err == nil {
		result.Inventory = inv
	}

	return result
}

// runInventoryProbe walks the full prefix recursively to compute object
// count, total bytes, and oldest/newest LastModified. Bounded by a
// generous timeout — for repos with millions of objects this would need
// to be paginated/sharded, but our largest current repos are <50k.
func runInventoryProbe(
	ctx context.Context, client *minio.Client, spec aretev1alpha1.BackupRepositorySpec,
) (*aretev1alpha1.InventoryStatus, error) {
	listCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	var (
		count   int64
		bytes   int64
		oldest  time.Time
		newest  time.Time
		hasTime bool
	)
	for obj := range client.ListObjects(listCtx, spec.S3.Bucket, minio.ListObjectsOptions{
		Prefix:    spec.S3.Prefix + "/",
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		count++
		bytes += obj.Size
		if !hasTime || obj.LastModified.Before(oldest) {
			oldest = obj.LastModified
		}
		if !hasTime || obj.LastModified.After(newest) {
			newest = obj.LastModified
		}
		hasTime = true
	}

	out := &aretev1alpha1.InventoryStatus{
		ObjectCount: count,
		TotalBytes:  *resource.NewQuantity(bytes, resource.BinarySI),
	}
	if hasTime {
		o := metav1.NewTime(oldest)
		n := metav1.NewTime(newest)
		out.OldestObject = &o
		out.NewestObject = &n
	}
	return out, nil
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

// detectFormatAndTimestamp does a format-specific lookup. Returns the
// detected format + version, the most recent successful backup's
// timestamp (drives BackupCurrent), and — for formats whose E1 can
// produce it — the full LatestBackup struct.
//
// Per-format ownership of claimedLatestBackup:
//   - wal-g: E1 owns it (sentinel is plaintext)
//   - restic: E2 owns it (E1 can only see encrypted file timestamps)
//   - barman: deferred
func detectFormatAndTimestamp(
	ctx context.Context, client *minio.Client, spec aretev1alpha1.BackupRepositorySpec,
) (format string, version string, lastSuccess *metav1.Time, latest *aretev1alpha1.LatestBackupStatus) {
	switch spec.Format {
	case aretev1alpha1.BackupFormatWalg:
		v, t, l := readWalgSentinel(ctx, client, spec)
		return "walg", v, t, l
	case aretev1alpha1.BackupFormatRestic:
		// detectedVersion stays empty for restic at E1 — restic encrypts
		// the config file, so version determination requires E2.
		// LatestBackup nil here; E2 populates it from `restic snapshots`.
		return "restic", "", readResticLatestSnapshotTime(ctx, client, spec), nil
	case aretev1alpha1.BackupFormatBarman:
		return "barman", "", nil, nil
	}
	return "", "", nil, nil
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
// parses the repo or doesn't).
//
// Wal-g sentinels are PLAINTEXT (encryption applies to basebackup data
// + WAL segments only). So E1 owns the full claimedLatestBackup struct
// for wal-g — name from the sentinel filename pattern, size + timestamp
// from the JSON body. E2 doesn't touch claimedLatestBackup for wal-g.
type walgSentinel struct {
	Version          int    `json:"Version"`   // sentinel format version (e.g. 2)
	PgVersion        int    `json:"PgVersion"` // packed: 160008 = "16.0.8"
	FinishTime       string `json:"FinishTime"`
	CompressedSize   int64  `json:"CompressedSize"`
	UncompressedSize int64  `json:"UncompressedSize"`
}

// readWalgSentinel parses the most-recent backup_stop_sentinel.json under
// <prefix>/basebackups_005/ to extract:
//   - detected version string (sentinel-vN/pg-X.Y.Z)
//   - claimedLastSuccessfulBackup timestamp (load-bearing for BackupCurrent)
//   - full claimedLatestBackup struct (name from filename, size + time
//     from sentinel JSON)
func readWalgSentinel(
	ctx context.Context, client *minio.Client, spec aretev1alpha1.BackupRepositorySpec,
) (string, *metav1.Time, *aretev1alpha1.LatestBackupStatus) {
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
			return "", nil, nil
		}
		if strings.HasSuffix(obj.Key, suffix) {
			sentinels = append(sentinels, obj)
		}
	}
	if len(sentinels) == 0 {
		return "", nil, nil
	}
	// Pick the most recent sentinel.
	sort.Slice(sentinels, func(i, j int) bool {
		return sentinels[i].LastModified.After(sentinels[j].LastModified)
	})
	latestKey := sentinels[0].Key

	body, err := getObjectBody(ctx, client, spec.S3.Bucket, latestKey)
	if err != nil {
		log.Error(err, "walg sentinel GET failed", "key", latestKey)
		return "", nil, nil
	}
	var s walgSentinel
	if err := json.Unmarshal(body, &s); err != nil {
		log.Error(err, "walg sentinel JSON parse failed", "key", latestKey)
		return "", nil, nil
	}

	version := fmt.Sprintf("sentinel-v%d/pg-%s", s.Version, formatPgVersion(s.PgVersion))
	createdAt := parseWalgTime(s.FinishTime, sentinels[0].LastModified)
	ts := metav1.NewTime(createdAt)

	// backup_name is the sentinel filename minus the path + suffix:
	//   basebackups_005/base_<wal-lsn>_backup_stop_sentinel.json
	//   →  base_<wal-lsn>
	name := strings.TrimSuffix(strings.TrimPrefix(latestKey, listPrefix), suffix)

	latest := &aretev1alpha1.LatestBackupStatus{
		Name:      name,
		CreatedAt: ts,
		SizeBytes: *resource.NewQuantity(s.CompressedSize, resource.BinarySI),
	}
	if s.UncompressedSize > 0 {
		uq := resource.NewQuantity(s.UncompressedSize, resource.BinarySI)
		latest.UncompressedSizeBytes = uq
	}
	return version, &ts, latest
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
