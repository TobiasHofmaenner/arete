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
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	aretev1alpha1 "github.com/TobiasHofmaenner/arete/api/v1alpha1"
)

// The tests in this file pin one rule above all: "the store has no backup"
// and "arete could not find out" are different answers. The first is a nil
// timestamp with a nil error; the second is always an error.

var (
	t0 = time.Date(2026, 11, 1, 3, 0, 0, 0, time.UTC)
	t1 = t0.Add(24 * time.Hour)
	t2 = t0.Add(48 * time.Hour)
)

const testPrefix = "tenant/nextcloud-v1/data"

func TestResticLatestSnapshot_EmptyIsNotAnError(t *testing.T) {
	f := newFakeS3(t)
	// A repository that exists but has no snapshot yet.
	f.put(testPrefix+"/config", "x", t0)

	got, err := readResticLatestSnapshotTime(context.Background(), f.client(t),
		f.spec(testPrefix, aretev1alpha1.BackupFormatRestic))
	if err != nil {
		t.Fatalf("an empty snapshots/ directory must not be an error, got %v", err)
	}
	if got != nil {
		t.Fatalf("expected no timestamp, got %v", got)
	}
}

func TestResticLatestSnapshot_NewestWins(t *testing.T) {
	f := newFakeS3(t)
	f.put(testPrefix+"/snapshots/aaa", "x", t1)
	f.put(testPrefix+"/snapshots/bbb", "x", t2)
	f.put(testPrefix+"/snapshots/ccc", "x", t0)
	// Lock files are newer than every snapshot and must not count.
	f.put(testPrefix+"/locks/zzz", "x", t2.Add(time.Hour))

	got, err := readResticLatestSnapshotTime(context.Background(), f.client(t),
		f.spec(testPrefix, aretev1alpha1.BackupFormatRestic))
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || !got.Time.Equal(t2) {
		t.Fatalf("expected %v, got %v", t2, got)
	}
}

func TestResticLatestSnapshot_ListFailureIsAnError(t *testing.T) {
	f := newFakeS3(t)
	f.put(testPrefix+"/snapshots/aaa", "x", t1)
	f.listErr[testPrefix+"/snapshots/"] = "AccessDenied"

	got, err := readResticLatestSnapshotTime(context.Background(), f.client(t),
		f.spec(testPrefix, aretev1alpha1.BackupFormatRestic))
	if err == nil {
		t.Fatal("a failed listing must be reported as an error, not as 'no snapshots'")
	}
	if got != nil {
		t.Fatalf("expected no timestamp alongside the error, got %v", got)
	}
}

const walgPrefix = "tenant/nextcloud-v1/postgres"

const validSentinel = `{"Version":2,"PgVersion":160008,` +
	`"FinishTime":"2026-11-02T03:00:07.123456Z",` +
	`"CompressedSize":1048576,"UncompressedSize":8388608}`

func TestWalgSentinel_NoneIsNotAnError(t *testing.T) {
	f := newFakeS3(t)
	// WAL is being archived but no base backup has completed.
	f.put(walgPrefix+"/wal_005/000000010000000000000001.lz4", "x", t0)

	version, ts, latest, err := readWalgSentinel(context.Background(), f.client(t),
		f.spec(walgPrefix, aretev1alpha1.BackupFormatWalg))
	if err != nil {
		t.Fatalf("no sentinel must not be an error, got %v", err)
	}
	if version != "" || ts != nil || latest != nil {
		t.Fatalf("expected empty result, got %q %v %v", version, ts, latest)
	}
}

func TestWalgSentinel_ParsesTheNewest(t *testing.T) {
	f := newFakeS3(t)
	older := strings.Replace(validSentinel, "2026-11-02", "2026-11-01", 1)
	f.put(walgPrefix+"/basebackups_005/base_000000010000000000000004_backup_stop_sentinel.json", older, t0)
	f.put(walgPrefix+"/basebackups_005/base_000000010000000000000009_backup_stop_sentinel.json", validSentinel, t1)
	// Per-backup data lives in a subdirectory and must not be mistaken for a sentinel.
	f.put(walgPrefix+"/basebackups_005/base_000000010000000000000009/tar_partitions/part_1.tar.lz4", "x", t2)

	version, ts, latest, err := readWalgSentinel(context.Background(), f.client(t),
		f.spec(walgPrefix, aretev1alpha1.BackupFormatWalg))
	if err != nil {
		t.Fatal(err)
	}
	if version != "sentinel-v2/pg-16.0.8" {
		t.Errorf("version = %q", version)
	}
	want := time.Date(2026, 11, 2, 3, 0, 7, 123456000, time.UTC)
	if ts == nil || !ts.Time.Equal(want) {
		t.Errorf("timestamp = %v, want %v", ts, want)
	}
	if latest == nil {
		t.Fatal("expected a latest-backup record")
	}
	if latest.Name != "base_000000010000000000000009" {
		t.Errorf("name = %q", latest.Name)
	}
	if got := latest.SizeBytes.Value(); got != 1048576 {
		t.Errorf("size = %d", got)
	}
	if latest.UncompressedSizeBytes == nil || latest.UncompressedSizeBytes.Value() != 8388608 {
		t.Errorf("uncompressed size = %v", latest.UncompressedSizeBytes)
	}
}

func TestWalgSentinel_FailuresAreErrors(t *testing.T) {
	key := walgPrefix + "/basebackups_005/base_000000010000000000000004_backup_stop_sentinel.json"
	cases := map[string]func(f *fakeS3){
		"listing denied": func(f *fakeS3) {
			f.put(key, validSentinel, t0)
			f.listErr[walgPrefix+"/basebackups_005/"] = "AccessDenied"
		},
		"sentinel unreadable": func(f *fakeS3) {
			f.put(key, validSentinel, t0)
			f.getErr[key] = "AccessDenied"
		},
		"sentinel is not JSON": func(f *fakeS3) {
			f.put(key, "<html>502 Bad Gateway</html>", t0)
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFakeS3(t)
			setup(f)
			_, ts, latest, err := readWalgSentinel(context.Background(), f.client(t),
				f.spec(walgPrefix, aretev1alpha1.BackupFormatWalg))
			if err == nil {
				t.Fatal("expected an error: this must not look like a repository without backups")
			}
			if ts != nil || latest != nil {
				t.Fatalf("expected no result alongside the error, got %v %v", ts, latest)
			}
		})
	}
}

func TestDetectFormat_UnimplementedFormatIsAnError(t *testing.T) {
	f := newFakeS3(t)
	_, _, ts, _, err := detectFormatAndTimestamp(context.Background(), f.client(t),
		f.spec("tenant/pg", aretev1alpha1.BackupFormatBarman))
	if !errors.Is(err, errFormatNotImplemented) {
		t.Fatalf("expected errFormatNotImplemented, got %v", err)
	}
	if ts != nil {
		t.Fatalf("expected no timestamp, got %v", ts)
	}
}

func TestProbeRepository(t *testing.T) {
	t.Run("healthy restic repository", func(t *testing.T) {
		f := newFakeS3(t)
		f.put(testPrefix+"/config", "cfg", t0)
		f.put(testPrefix+"/snapshots/aaa", "snap", t1)
		f.put(testPrefix+"/data/00/pack", "0123456789", t1)

		p := probeRepository(context.Background(),
			f.spec(testPrefix, aretev1alpha1.BackupFormatRestic), S3Credentials{AccessKeyID: "k", SecretAccessKey: "s"}, true)
		if !p.Reachable {
			t.Fatalf("expected reachable, got %s: %s", p.ReachableReason, p.ReachableMessage)
		}
		if p.BackupStateErr != nil {
			t.Fatalf("unexpected state error: %v", p.BackupStateErr)
		}
		if p.LastSuccessfulBackup == nil || !p.LastSuccessfulBackup.Time.Equal(t1) {
			t.Errorf("last backup = %v, want %v", p.LastSuccessfulBackup, t1)
		}
		if p.Inventory == nil || p.Inventory.ObjectCount != 3 || p.Inventory.TotalBytes.Value() != 17 {
			t.Errorf("inventory = %+v", p.Inventory)
		}
		if p.Inventory != nil && (!p.Inventory.OldestObject.Time.Equal(t0) || !p.Inventory.NewestObject.Time.Equal(t1)) {
			t.Errorf("inventory times = %v .. %v", p.Inventory.OldestObject, p.Inventory.NewestObject)
		}
	})

	t.Run("empty prefix is reachable and has no backup", func(t *testing.T) {
		f := newFakeS3(t)
		p := probeRepository(context.Background(),
			f.spec(testPrefix, aretev1alpha1.BackupFormatRestic), S3Credentials{AccessKeyID: "k", SecretAccessKey: "s"}, true)
		if !p.Reachable || p.BackupStateErr != nil || p.LastSuccessfulBackup != nil {
			t.Fatalf("reachable=%v err=%v last=%v", p.Reachable, p.BackupStateErr, p.LastSuccessfulBackup)
		}
		if p.Inventory == nil || p.Inventory.ObjectCount != 0 {
			t.Errorf("inventory = %+v", p.Inventory)
		}
	})

	t.Run("reachable but the backup state cannot be read", func(t *testing.T) {
		f := newFakeS3(t)
		f.put(testPrefix+"/snapshots/aaa", "snap", t1)
		f.listErr[testPrefix+"/snapshots/"] = "AccessDenied"

		p := probeRepository(context.Background(),
			f.spec(testPrefix, aretev1alpha1.BackupFormatRestic), S3Credentials{AccessKeyID: "k", SecretAccessKey: "s"}, true)
		if !p.Reachable {
			t.Fatalf("expected reachable, got %s", p.ReachableReason)
		}
		if p.BackupStateErr == nil {
			t.Fatal("expected a state error")
		}
		if p.LastSuccessfulBackup != nil {
			t.Fatalf("expected no timestamp, got %v", p.LastSuccessfulBackup)
		}
	})

	t.Run("missing bucket", func(t *testing.T) {
		f := newFakeS3(t)
		f.bucketErr = "NoSuchBucket"
		p := probeRepository(context.Background(),
			f.spec(testPrefix, aretev1alpha1.BackupFormatRestic), S3Credentials{AccessKeyID: "k", SecretAccessKey: "s"}, true)
		if p.Reachable || p.ReachableReason != aretev1alpha1.ReasonBucketNotFound {
			t.Fatalf("reachable=%v reason=%s", p.Reachable, p.ReachableReason)
		}
	})

	t.Run("rejected credentials", func(t *testing.T) {
		f := newFakeS3(t)
		f.listErr[testPrefix+"/"] = "AccessDenied"
		p := probeRepository(context.Background(),
			f.spec(testPrefix, aretev1alpha1.BackupFormatRestic), S3Credentials{AccessKeyID: "k", SecretAccessKey: "s"}, true)
		if p.Reachable || p.ReachableReason != aretev1alpha1.ReasonCredentialsRejected {
			t.Fatalf("reachable=%v reason=%s", p.Reachable, p.ReachableReason)
		}
	})
}

func TestCheckDeclaredBucketSettings(t *testing.T) {
	cases := []struct {
		name          string
		lock, enc     string // fake server state
		reqLock, reqE bool
		wantOK        bool
		wantReason    string
		wantChecked   []string
	}{
		{name: "nothing required", wantOK: true},
		{name: "lock required and enabled", lock: "enabled", reqLock: true, wantOK: true, wantChecked: []string{"object-lock"}},
		{name: "lock required but not configured", reqLock: true, wantReason: aretev1alpha1.ReasonObjectLockMissing},
		{
			// The credentials may read objects but not the bucket's
			// configuration. That is not evidence the lock is missing.
			name: "lock required but not readable", lock: "AccessDenied", reqLock: true,
			wantReason: aretev1alpha1.ReasonCredentialsRejected,
		},
		{name: "both required and enabled", lock: "enabled", enc: "enabled", reqLock: true, reqE: true,
			wantOK: true, wantChecked: []string{"object-lock", "bucket-encryption"}},
		{name: "encryption required but not configured", reqE: true, wantReason: aretev1alpha1.ReasonBucketEncryptionMissing},
		{name: "encryption required but not readable", enc: "AccessDenied", reqE: true,
			wantReason: aretev1alpha1.ReasonCredentialsRejected},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeS3(t)
			f.objectLock, f.encryption = tc.lock, tc.enc
			spec := f.spec(testPrefix, aretev1alpha1.BackupFormatRestic)
			spec.S3.RequireObjectLock, spec.S3.RequireBucketEncryption = tc.reqLock, tc.reqE

			checked, ok, reason, msg := checkDeclaredBucketSettings(context.Background(), f.client(t), spec)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v (%s: %s)", ok, reason, msg)
			}
			if !tc.wantOK && reason != tc.wantReason {
				t.Errorf("reason = %q, want %q (%s)", reason, tc.wantReason, msg)
			}
			if tc.wantOK && !slices.Equal(checked, tc.wantChecked) {
				t.Errorf("checked = %v, want %v", checked, tc.wantChecked)
			}
		})
	}
}

func TestCheckBucketSecurity_RejectsPlainHTTP(t *testing.T) {
	f := newFakeS3(t) // the fake serves plain http
	ok, reason, _ := checkBucketSecurity(context.Background(), f.client(t),
		f.spec(testPrefix, aretev1alpha1.BackupFormatRestic))
	if ok || reason != aretev1alpha1.ReasonInsecureEndpoint {
		t.Fatalf("ok=%v reason=%s", ok, reason)
	}
}

func TestClassifyS3Error(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{minio.ErrorResponse{Code: "NoSuchBucket"}, aretev1alpha1.ReasonBucketNotFound},
		{minio.ErrorResponse{Code: "AccessDenied"}, aretev1alpha1.ReasonCredentialsRejected},
		{minio.ErrorResponse{Code: "InvalidAccessKeyId"}, aretev1alpha1.ReasonCredentialsRejected},
		{minio.ErrorResponse{Code: "SignatureDoesNotMatch"}, aretev1alpha1.ReasonCredentialsRejected},
		{minio.ErrorResponse{Code: "SlowDown"}, aretev1alpha1.ReasonS3APIError},
		{errors.New("dial tcp: connection refused"), aretev1alpha1.ReasonS3Unreachable},
	}
	for _, tc := range cases {
		if got, _ := classifyS3Error(tc.err); got != tc.want {
			t.Errorf("classifyS3Error(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

func TestFormatPgVersion(t *testing.T) {
	for in, want := range map[int]string{0: "unknown", 160008: "16.0.8", 170002: "17.0.2", 150013: "15.0.13"} {
		if got := formatPgVersion(in); got != want {
			t.Errorf("formatPgVersion(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestParseWalgTime(t *testing.T) {
	fallback := t0
	cases := []struct {
		in   string
		want time.Time
	}{
		{"", fallback},
		{"not a time", fallback},
		{"2026-11-02T03:00:07Z", time.Date(2026, 11, 2, 3, 0, 7, 0, time.UTC)},
		{"2026-11-02T03:00:07.123456Z", time.Date(2026, 11, 2, 3, 0, 7, 123456000, time.UTC)},
		{"2026-11-02T04:00:07+01:00", time.Date(2026, 11, 2, 3, 0, 7, 0, time.UTC)},
	}
	for _, tc := range cases {
		if got := parseWalgTime(tc.in, fallback); !got.Equal(tc.want) {
			t.Errorf("parseWalgTime(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestBuildS3Client_RejectsBadEndpoints(t *testing.T) {
	for _, endpoint := range []string{"", "not a url at all", "https://"} {
		if _, err := buildS3Client(aretev1alpha1.S3Source{Endpoint: endpoint, Region: "x"}, S3Credentials{}); err == nil {
			t.Errorf("endpoint %q: expected an error", endpoint)
		}
	}
}

func TestProbeRepository_InventoryOnlyWhenRequested(t *testing.T) {
	f := newFakeS3(t)
	f.put(testPrefix+"/config", "cfg", t0)
	f.put(testPrefix+"/snapshots/aaa", "snap", t1)
	spec := f.spec(testPrefix, aretev1alpha1.BackupFormatRestic)
	creds := S3Credentials{AccessKeyID: "k", SecretAccessKey: "s"}

	p := probeRepository(context.Background(), spec, creds, false)
	if !p.Reachable || p.LastSuccessfulBackup == nil {
		t.Fatalf("a probe without inventory must still find the backup: reachable=%v last=%v",
			p.Reachable, p.LastSuccessfulBackup)
	}
	if p.Inventory != nil {
		t.Errorf("inventory was produced although not requested: %+v", p.Inventory)
	}
	if f.recursiveLists != 0 {
		t.Errorf("a probe without inventory walked the whole prefix %d time(s)", f.recursiveLists)
	}

	p = probeRepository(context.Background(), spec, creds, true)
	if p.Inventory == nil || p.Inventory.ObjectCount != 2 {
		t.Errorf("inventory = %+v, want 2 objects", p.Inventory)
	}
	if f.recursiveLists != 1 {
		t.Errorf("expected exactly one full walk, got %d", f.recursiveLists)
	}
}
