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
// stdout. Per-format ownership of claimedLatestBackup means only formats
// whose E2 owns the field have a parser here:
//   - wal-g: NOT here — E1 sentinel parsing owns the field
//     (sentinels are plaintext + cheap to read).
//   - restic: here — E1 can't see beyond encrypted file timestamps,
//     so E2 parses `restic snapshots --json` for the full detail.
//
// Best-effort: returns nil if the output is missing or malformed
// (MetadataValid success/failure is independently set from exit code).
func parseE2Output(format aretev1alpha1.BackupFormat, logs string) *aretev1alpha1.LatestBackupStatus {
	switch format {
	case aretev1alpha1.BackupFormatRestic:
		return parseResticSnapshotsJSON(logs)
	}
	return nil
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
	var snaps []resticSnapshot
	if !findAndUnmarshalJSON(logs, '[', &snaps) || len(snaps) == 0 {
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

// findAndUnmarshalJSON tries every position of `delim` in logs as the
// start of a balanced JSON value, attempting to unmarshal each candidate
// into out. Returns true on first successful unmarshal.
//
// This handles validators (notably restic) that print non-JSON lines
// containing brackets — e.g. progress like "[0:00] 100.00% 13/13" —
// before the actual JSON output. A naive "find first [" would lock onto
// the wrong substring.
func findAndUnmarshalJSON(logs string, delim byte, out any) bool {
	for offset := 0; offset < len(logs); {
		idx := strings.IndexByte(logs[offset:], delim)
		if idx < 0 {
			return false
		}
		start := offset + idx
		body := extractBalanced(logs, start, delim)
		if body != "" && json.Unmarshal([]byte(body), out) == nil {
			return true
		}
		offset = start + 1
	}
	return false
}

// extractBalanced returns the substring starting at `start` that is a
// balanced JSON value bounded by `delim` and its matching closer.
// Tracks string literals (and escapes) to avoid counting brackets that
// appear inside JSON strings.
func extractBalanced(logs string, start int, delim byte) string {
	closer := byte('}')
	if delim == '[' {
		closer = ']'
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
		case closer:
			depth--
			if depth == 0 {
				return logs[start : i+1]
			}
		}
	}
	return ""
}
