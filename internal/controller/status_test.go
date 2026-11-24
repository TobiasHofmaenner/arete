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
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aretev1alpha1 "github.com/TobiasHofmaenner/arete/api/v1alpha1"
	aretemetrics "github.com/TobiasHofmaenner/arete/internal/metrics"
)

func mtime(t time.Time) *metav1.Time {
	m := metav1.NewTime(t)
	return &m
}

// knownStatus is a status that already holds a backup, as left by an
// earlier successful probe.
func knownStatus() aretev1alpha1.BackupRepositoryStatus {
	return aretev1alpha1.BackupRepositoryStatus{
		DetectedFormat:              "restic",
		ClaimedLastSuccessfulBackup: mtime(t1),
		ClaimedLatestBackup:         &aretev1alpha1.LatestBackupStatus{Name: "abc123", CreatedAt: *mtime(t1)},
		ObservedInventory:           &aretev1alpha1.InventoryStatus{ObjectCount: 42},
	}
}

func TestApplyProbeToStatus(t *testing.T) {
	t.Run("an unreachable store erases nothing", func(t *testing.T) {
		st := knownStatus()
		applyProbeToStatus(&st, probeResult{Reachable: false})
		if st.ClaimedLastSuccessfulBackup == nil || !st.ClaimedLastSuccessfulBackup.Time.Equal(t1) {
			t.Errorf("last backup was erased: %v", st.ClaimedLastSuccessfulBackup)
		}
		if st.DetectedFormat != "restic" || st.ClaimedLatestBackup == nil || st.ObservedInventory == nil {
			t.Errorf("known state was erased: %+v", st)
		}
	})

	t.Run("a failed lookup erases nothing", func(t *testing.T) {
		st := knownStatus()
		applyProbeToStatus(&st, probeResult{
			Reachable: true, DetectedFormat: "restic", BackupStateErr: errors.New("list denied"),
		})
		if st.ClaimedLastSuccessfulBackup == nil || !st.ClaimedLastSuccessfulBackup.Time.Equal(t1) {
			t.Errorf("last backup was erased by a failed lookup: %v", st.ClaimedLastSuccessfulBackup)
		}
	})

	t.Run("a completed lookup that finds nothing is written through", func(t *testing.T) {
		st := knownStatus()
		applyProbeToStatus(&st, probeResult{Reachable: true, DetectedFormat: "restic"})
		if st.ClaimedLastSuccessfulBackup != nil {
			t.Errorf("the store answered 'no backup'; status must say so, got %v", st.ClaimedLastSuccessfulBackup)
		}
		// restic: the latest-backup record belongs to E2 and stays.
		if st.ClaimedLatestBackup == nil {
			t.Error("restic latest-backup record is owned by E2 and must not be cleared by the probe")
		}
	})

	t.Run("a newer backup replaces the old one", func(t *testing.T) {
		st := knownStatus()
		applyProbeToStatus(&st, probeResult{Reachable: true, DetectedFormat: "restic", LastSuccessfulBackup: mtime(t2)})
		if !st.ClaimedLastSuccessfulBackup.Time.Equal(t2) {
			t.Errorf("last backup = %v, want %v", st.ClaimedLastSuccessfulBackup, t2)
		}
	})

	t.Run("wal-g: the probe owns the latest-backup record, including its absence", func(t *testing.T) {
		st := knownStatus()
		st.DetectedFormat = "walg"
		applyProbeToStatus(&st, probeResult{Reachable: true, DetectedFormat: "walg"})
		if st.ClaimedLatestBackup != nil {
			t.Errorf("wal-g repository without a sentinel must clear the record, got %+v", st.ClaimedLatestBackup)
		}
	})

	t.Run("inventory is kept when the listing failed", func(t *testing.T) {
		st := knownStatus()
		applyProbeToStatus(&st, probeResult{Reachable: true, DetectedFormat: "restic", Inventory: nil})
		if st.ObservedInventory == nil || st.ObservedInventory.ObjectCount != 42 {
			t.Errorf("inventory = %+v", st.ObservedInventory)
		}
	})
}

func testRepository(lastBackup *metav1.Time) *aretev1alpha1.BackupRepository {
	br := &aretev1alpha1.BackupRepository{}
	br.Name = "tenant-data"
	br.Generation = 7
	br.Spec.Format = aretev1alpha1.BackupFormatRestic
	br.Spec.MaxBackupLag = metav1.Duration{Duration: 25 * time.Hour}
	br.Status.ClaimedLastSuccessfulBackup = lastBackup
	return br
}

func TestBackupCurrentCondition(t *testing.T) {
	now := t2
	cases := []struct {
		name       string
		last       *metav1.Time
		probe      probeResult
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name: "recent backup", last: mtime(now.Add(-2 * time.Hour)),
			probe:      probeResult{Reachable: true},
			wantStatus: metav1.ConditionTrue, wantReason: aretev1alpha1.ReasonProbeSucceeded,
		},
		{
			name: "backup older than the allowed lag", last: mtime(now.Add(-26 * time.Hour)),
			probe:      probeResult{Reachable: true},
			wantStatus: metav1.ConditionFalse, wantReason: aretev1alpha1.ReasonBackupLagExceeded,
		},
		{
			name: "the store answered and there is no backup", last: nil,
			probe:      probeResult{Reachable: true},
			wantStatus: metav1.ConditionFalse, wantReason: aretev1alpha1.ReasonRepositoryEmpty,
		},
		{
			// The case this function exists for: a failed lookup must not be
			// reported as an empty repository, even when no backup is known.
			name: "lookup failed and nothing is known", last: nil,
			probe:      probeResult{Reachable: true, BackupStateErr: errors.New("list denied")},
			wantStatus: metav1.ConditionUnknown, wantReason: aretev1alpha1.ReasonBackupStateUnreadable,
		},
		{
			name: "lookup failed although a recent backup is known", last: mtime(now.Add(-time.Hour)),
			probe:      probeResult{Reachable: true, BackupStateErr: errors.New("list denied")},
			wantStatus: metav1.ConditionUnknown, wantReason: aretev1alpha1.ReasonBackupStateUnreadable,
		},
		{
			name: "format not implemented", last: nil,
			probe:      probeResult{Reachable: true, BackupStateErr: errFormatNotImplemented},
			wantStatus: metav1.ConditionUnknown, wantReason: aretev1alpha1.ReasonFormatNotImplemented,
		},
		{
			name: "store unreachable", last: mtime(now.Add(-time.Hour)),
			probe:      probeResult{Reachable: false},
			wantStatus: metav1.ConditionUnknown, wantReason: aretev1alpha1.ReasonS3Unreachable,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := backupCurrentCondition(testRepository(tc.last), tc.probe, now)
			if got.Status != tc.wantStatus || got.Reason != tc.wantReason {
				t.Errorf("got %s/%s (%s), want %s/%s", got.Status, got.Reason, got.Message, tc.wantStatus, tc.wantReason)
			}
		})
	}
}

func TestComputeBackupCurrent_LagBoundaryIsInclusive(t *testing.T) {
	now := t2
	br := testRepository(mtime(now.Add(-25 * time.Hour)))
	if got := computeBackupCurrent(br, now); got.Status != metav1.ConditionTrue {
		t.Errorf("a backup exactly maxBackupLag old is still current, got %s", got.Status)
	}
}

func TestComputeSizeWithinBudget(t *testing.T) {
	budget := resource.MustParse("10Gi")
	br := testRepository(nil)
	br.Spec.ExpectedSizeBudget = &budget

	if got := computeSizeWithinBudget(br); got.Status != metav1.ConditionUnknown {
		t.Errorf("no inventory yet: got %s, want Unknown", got.Status)
	}
	br.Status.ObservedInventory = &aretev1alpha1.InventoryStatus{TotalBytes: resource.MustParse("10Gi")}
	if got := computeSizeWithinBudget(br); got.Status != metav1.ConditionTrue {
		t.Errorf("exactly at budget: got %s, want True", got.Status)
	}
	br.Status.ObservedInventory.TotalBytes = resource.MustParse("11Gi")
	if got := computeSizeWithinBudget(br); got.Status != metav1.ConditionFalse || got.Reason != aretev1alpha1.ReasonSizeBudgetExceeded {
		t.Errorf("over budget: got %s/%s", got.Status, got.Reason)
	}
}

func cond(condType string, status metav1.ConditionStatus) metav1.Condition {
	return metav1.Condition{Type: condType, Status: status, Reason: "Test", Message: condType + " message"}
}

func TestRollupAND(t *testing.T) {
	const a, b, c = "A", "B", "C"
	cases := []struct {
		name string
		in   []metav1.Condition
		want metav1.ConditionStatus
	}{
		{"all true", []metav1.Condition{cond(a, metav1.ConditionTrue), cond(b, metav1.ConditionTrue)}, metav1.ConditionTrue},
		{"one false wins over unknown", []metav1.Condition{
			cond(a, metav1.ConditionTrue), cond(b, metav1.ConditionUnknown), cond(c, metav1.ConditionFalse),
		}, metav1.ConditionFalse},
		{"unknown wins over true", []metav1.Condition{cond(a, metav1.ConditionTrue), cond(b, metav1.ConditionUnknown)}, metav1.ConditionUnknown},
		{"absent conditions are skipped", []metav1.Condition{cond(a, metav1.ConditionTrue)}, metav1.ConditionTrue},
		{"nothing present is unknown, never true", nil, metav1.ConditionUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rollupAND(tc.in, a, b, c); got.Status != tc.want {
				t.Errorf("got %s, want %s", got.Status, tc.want)
			}
		})
	}
}

func condStatus(br *aretev1alpha1.BackupRepository, condType string) metav1.ConditionStatus {
	c := apimeta.FindStatusCondition(br.Status.Conditions, condType)
	if c == nil {
		return "(absent)"
	}
	return c.Status
}

func TestApplyConditions(t *testing.T) {
	r := &BackupRepositoryReconciler{}
	ok := condTrue(aretev1alpha1.ReasonProbeSucceeded, "ok")

	t.Run("everything passing rolls up to healthy", func(t *testing.T) {
		br := testRepository(mtime(t1))
		r.applyConditions(br, conditionInputs{
			reachable: ok, bucketSecurityValid: ok, backupCurrent: ok, metadataValid: ok,
		})
		for _, ct := range []string{
			aretev1alpha1.ConditionProbeHealthy, aretev1alpha1.ConditionValidationHealthy, aretev1alpha1.ConditionHealthy,
		} {
			if got := condStatus(br, ct); got != metav1.ConditionTrue {
				t.Errorf("%s = %s, want True", ct, got)
			}
		}
		// Levels that are not enabled must not appear at all.
		if got := condStatus(br, aretev1alpha1.ConditionSampledIntegrityValid); got != "(absent)" {
			t.Errorf("SampledIntegrityValid = %s, want absent when E3 is not enabled", got)
		}
	})

	t.Run("an unknown backup state makes the repository unknown, not healthy", func(t *testing.T) {
		br := testRepository(mtime(t1))
		r.applyConditions(br, conditionInputs{
			reachable: ok, bucketSecurityValid: ok, metadataValid: ok,
			backupCurrent: condUnknown(aretev1alpha1.ReasonBackupStateUnreadable, "list denied"),
		})
		if got := condStatus(br, aretev1alpha1.ConditionHealthy); got != metav1.ConditionUnknown {
			t.Errorf("Healthy = %s, want Unknown", got)
		}
	})

	t.Run("a failed validation makes the repository unhealthy", func(t *testing.T) {
		br := testRepository(mtime(t1))
		r.applyConditions(br, conditionInputs{
			reachable: ok, bucketSecurityValid: ok, backupCurrent: ok,
			metadataValid: condFalse(aretev1alpha1.ReasonMetadataValidationFailed, "restic check failed"),
		})
		if got := condStatus(br, aretev1alpha1.ConditionHealthy); got != metav1.ConditionFalse {
			t.Errorf("Healthy = %s, want False", got)
		}
	})

	t.Run("no validation result yet is unknown", func(t *testing.T) {
		br := testRepository(mtime(t1))
		r.applyConditions(br, conditionInputs{reachable: ok, bucketSecurityValid: ok, backupCurrent: ok})
		if got := condStatus(br, aretev1alpha1.ConditionMetadataValid); got != metav1.ConditionUnknown {
			t.Errorf("MetadataValid = %s, want Unknown", got)
		}
		if got := condStatus(br, aretev1alpha1.ConditionHealthy); got != metav1.ConditionUnknown {
			t.Errorf("Healthy = %s, want Unknown", got)
		}
	})

	t.Run("every condition carries the generation it describes", func(t *testing.T) {
		br := testRepository(mtime(t1))
		r.applyConditions(br, conditionInputs{reachable: ok, bucketSecurityValid: ok, backupCurrent: ok, metadataValid: ok})
		if len(br.Status.Conditions) == 0 {
			t.Fatal("no conditions set")
		}
		for _, c := range br.Status.Conditions {
			if c.ObservedGeneration != br.Generation {
				t.Errorf("%s has observedGeneration %d, want %d", c.Type, c.ObservedGeneration, br.Generation)
			}
		}
	})
}

// seriesFor returns the label sets of every series of c whose "br" label is name.
func seriesFor(t *testing.T, c prometheus.Collector, name string) []map[string]string {
	t.Helper()
	ch := make(chan prometheus.Metric, 256)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	var out []map[string]string
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			t.Fatalf("write metric: %v", err)
		}
		labels := map[string]string{}
		for _, l := range d.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		if labels["br"] == name {
			out = append(out, labels)
		}
	}
	return out
}

func TestPublishMetrics_DropsSeriesThatNoLongerApply(t *testing.T) {
	br := testRepository(mtime(time.Now().Add(-time.Hour)))
	br.Name = "metrics-stale-series"
	br.Status.Conditions = []metav1.Condition{
		cond(aretev1alpha1.ConditionHealthy, metav1.ConditionTrue),
		cond(aretev1alpha1.ConditionSampledIntegrityValid, metav1.ConditionTrue),
	}
	publishMetrics(br)
	if n := len(seriesFor(t, aretemetrics.BackupAgeSeconds, br.Name)); n != 1 {
		t.Fatalf("backup age series = %d, want 1", n)
	}
	if n := len(seriesFor(t, aretemetrics.ConditionState, br.Name)); n != 2 {
		t.Fatalf("condition series = %d, want 2", n)
	}

	// The backup disappears and E3 is switched off.
	br.Status.ClaimedLastSuccessfulBackup = nil
	br.Status.Conditions = []metav1.Condition{cond(aretev1alpha1.ConditionHealthy, metav1.ConditionFalse)}
	publishMetrics(br)

	if n := len(seriesFor(t, aretemetrics.BackupAgeSeconds, br.Name)); n != 0 {
		t.Errorf("backup age must not stay frozen at its last value; series = %d", n)
	}
	got := seriesFor(t, aretemetrics.ConditionState, br.Name)
	if len(got) != 1 || got[0]["condition"] != aretev1alpha1.ConditionHealthy {
		t.Errorf("condition series = %v, want only Healthy", got)
	}
}

func TestDeleteRepository_RemovesAllItsSeries(t *testing.T) {
	br := testRepository(mtime(time.Now().Add(-time.Hour)))
	br.Name = "metrics-deleted-repo"
	br.Status.ObservedInventory = &aretev1alpha1.InventoryStatus{ObjectCount: 3, TotalBytes: resource.MustParse("1Ki")}
	br.Status.Conditions = []metav1.Condition{cond(aretev1alpha1.ConditionHealthy, metav1.ConditionTrue)}
	publishMetrics(br)
	aretemetrics.ValidationRunsTotal.WithLabelValues(br.Name, "restic", "e2", "success").Inc()

	other := testRepository(mtime(time.Now()))
	other.Name = "metrics-surviving-repo"
	other.Status.Conditions = []metav1.Condition{cond(aretev1alpha1.ConditionHealthy, metav1.ConditionTrue)}
	publishMetrics(other)

	aretemetrics.DeleteRepository(br.Name)

	for name, c := range map[string]prometheus.Collector{
		"backup age": aretemetrics.BackupAgeSeconds, "condition state": aretemetrics.ConditionState,
		"inventory objects": aretemetrics.InventoryObjects, "inventory bytes": aretemetrics.InventoryBytes,
		"validation runs": aretemetrics.ValidationRunsTotal,
	} {
		if n := len(seriesFor(t, c, br.Name)); n != 0 {
			t.Errorf("%s: %d series left for the deleted repository", name, n)
		}
	}
	if n := len(seriesFor(t, aretemetrics.ConditionState, other.Name)); n != 1 {
		t.Errorf("another repository's series were removed too (left: %d)", n)
	}
}

func TestSecretKeyFor(t *testing.T) {
	mapping := map[string]string{"AWS_ACCESS_KEY_ID": "ACCESS_KEY_ID", "AWS_SESSION_TOKEN": ""}
	for canonical, want := range map[string]string{
		"AWS_ACCESS_KEY_ID":     "ACCESS_KEY_ID",         // remapped
		"AWS_SECRET_ACCESS_KEY": "AWS_SECRET_ACCESS_KEY", // not in the mapping
		"AWS_SESSION_TOKEN":     "AWS_SESSION_TOKEN",     // empty mapping value is ignored
	} {
		if got := secretKeyFor(canonical, mapping); got != want {
			t.Errorf("secretKeyFor(%q) = %q, want %q", canonical, got, want)
		}
	}
}

func TestLastLine(t *testing.T) {
	long := make([]byte, 300)
	for i := range long {
		long[i] = 'x'
	}
	cases := map[string]string{
		"":                         "(no output)",
		"\n\n":                     "(no output)",
		"first\nsecond\n":          "second",
		"first\n   \n  last  \n\n": "last",
		string(long):               string(long[:200]) + "...",
	}
	for in, want := range cases {
		if got := lastLine(in); got != want {
			t.Errorf("lastLine(%.20q) = %.30q, want %.30q", in, got, want)
		}
	}
}
