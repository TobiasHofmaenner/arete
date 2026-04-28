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
	"encoding/json"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aretev1alpha1 "github.com/TobiasHofmaenner/arete/api/v1alpha1"
)

// parseE2Output extracts a LatestBackupStatus from the validator pod's
// stdout. Each format's E2 command emits a structured JSON line for the
// most recent backup; we scan the captured logs for it. Best-effort —
// returns nil if the output is empty or malformed (the condition's
// success state is set by the caller from the Job exit code).
func parseE2Output(format aretev1alpha1.BackupFormat, logs string) *aretev1alpha1.LatestBackupStatus {
	switch format {
	case aretev1alpha1.BackupFormatWalg:
		return parseWalgBackupListJSON(logs)
	case aretev1alpha1.BackupFormatRestic:
		return parseResticSnapshotsJSON(logs)
	}
	return nil
}

// --- wal-g ---

// walgBackupListEntry mirrors one element of `wal-g backup-list --detail
// --json`'s output. Only the fields we actually use; wal-g's struct has
// many more (LSN markers, system identifier, etc.).
type walgBackupListEntry struct {
	BackupName       string `json:"backup_name"`
	Time             string `json:"time"` // RFC3339-ish
	StartTime        string `json:"start_time"`
	FinishTime       string `json:"finish_time"`
	CompressedSize   int64  `json:"compressed_size"`
	UncompressedSize int64  `json:"uncompressed_size"`
}

func parseWalgBackupListJSON(logs string) *aretev1alpha1.LatestBackupStatus {
	// `wal-g backup-list --detail --json` emits a JSON array on stdout.
	// scanForJSON finds the array (validators may print other lines).
	body := scanForJSON(logs, '[')
	if body == "" {
		return nil
	}
	var entries []walgBackupListEntry
	if err := json.Unmarshal([]byte(body), &entries); err != nil {
		return nil
	}
	if len(entries) == 0 {
		return nil
	}
	// Pick the latest by FinishTime (fall back to Time then list order).
	latest := entries[len(entries)-1]
	for _, e := range entries {
		if compareWalgTimes(e.finishOrTime(), latest.finishOrTime()) > 0 {
			latest = e
		}
	}
	createdAt := parseWalgTime(latest.finishOrTime(), time.Time{})
	out := &aretev1alpha1.LatestBackupStatus{
		Name:      latest.BackupName,
		CreatedAt: metav1.NewTime(createdAt),
		SizeBytes: *resource.NewQuantity(latest.CompressedSize, resource.BinarySI),
	}
	if latest.UncompressedSize > 0 {
		uq := resource.NewQuantity(latest.UncompressedSize, resource.BinarySI)
		out.UncompressedSizeBytes = uq
	}
	return out
}

func (e walgBackupListEntry) finishOrTime() string {
	if e.FinishTime != "" {
		return e.FinishTime
	}
	return e.Time
}

func compareWalgTimes(a, b string) int {
	ta := parseWalgTime(a, time.Time{})
	tb := parseWalgTime(b, time.Time{})
	switch {
	case ta.After(tb):
		return 1
	case ta.Before(tb):
		return -1
	}
	return 0
}

// --- restic ---

// resticSnapshot mirrors one element of `restic snapshots --json`'s
// output (relevant fields only). Summary contains the per-backup byte
// totals when the snapshot was created with the modern restic versions.
type resticSnapshot struct {
	ID       string        `json:"short_id"`
	LongID   string        `json:"id"`
	Time     string        `json:"time"`
	Hostname string        `json:"hostname"`
	Paths    []string      `json:"paths"`
	Tags     []string      `json:"tags"`
	Summary  resticSnapSum `json:"summary"`
}

type resticSnapSum struct {
	TotalBytesProcessed int64 `json:"total_bytes_processed"`
	DataAddedPacked     int64 `json:"data_added_packed"`
}

func parseResticSnapshotsJSON(logs string) *aretev1alpha1.LatestBackupStatus {
	// `restic snapshots --json` emits a JSON array.
	body := scanForJSON(logs, '[')
	if body == "" {
		return nil
	}
	var snaps []resticSnapshot
	if err := json.Unmarshal([]byte(body), &snaps); err != nil {
		return nil
	}
	if len(snaps) == 0 {
		return nil
	}
	// Pick the latest by Time.
	latest := snaps[len(snaps)-1]
	for _, s := range snaps {
		if parseResticTime(s.Time).After(parseResticTime(latest.Time)) {
			latest = s
		}
	}
	id := latest.ID
	if id == "" {
		id = latest.LongID
	}
	out := &aretev1alpha1.LatestBackupStatus{
		Name:      id,
		CreatedAt: metav1.NewTime(parseResticTime(latest.Time)),
	}
	if latest.Summary.DataAddedPacked > 0 {
		out.SizeBytes = *resource.NewQuantity(latest.Summary.DataAddedPacked, resource.BinarySI)
	}
	if latest.Summary.TotalBytesProcessed > 0 {
		uq := resource.NewQuantity(latest.Summary.TotalBytesProcessed, resource.BinarySI)
		out.UncompressedSizeBytes = uq
	}
	return out
}

func parseResticTime(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02T15:04:05Z07:00",
	} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t
		}
	}
	return time.Time{}
}

// --- shared ---

// scanForJSON extracts the longest top-level JSON value from log output
// that begins with the given delimiter ('{' or '['). Validators may
// print non-JSON lines (warnings, status messages) before/after the
// structured output; this finds the first balanced JSON value.
func scanForJSON(logs string, delim byte) string {
	close := byte('}')
	if delim == '[' {
		close = ']'
	}
	start := strings.IndexByte(logs, delim)
	if start < 0 {
		return ""
	}
	depth := 0
	inStr := false
	escaped := false
	for i := start; i < len(logs); i++ {
		c := logs[i]
		if escaped {
			escaped = false
			continue
		}
		if inStr {
			switch c {
			case '\\':
				escaped = true
			case '"':
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case delim:
			depth++
		case close:
			depth--
			if depth == 0 {
				return logs[start : i+1]
			}
		}
	}
	return ""
}
