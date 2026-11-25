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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aretev1alpha1 "github.com/TobiasHofmaenner/arete/api/v1alpha1"
)

const jobTestNamespace = "tenant"

func jobTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := aretev1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// jobTestRepository is a restic repository with E2 every 6h and E3 every 6h.
func jobTestRepository() *aretev1alpha1.BackupRepository {
	br := &aretev1alpha1.BackupRepository{}
	br.Name = "tenant-data"
	br.UID = types.UID("11111111-1111-1111-1111-111111111111")
	br.Spec.Format = aretev1alpha1.BackupFormatRestic
	br.Spec.S3 = aretev1alpha1.S3Source{
		Endpoint: "https://s3.example.com", Region: "us-east-1", Bucket: "backups", Prefix: "tenant/data",
		CredentialsSecret: aretev1alpha1.SecretReference{Name: "creds", Namespace: jobTestNamespace},
	}
	br.Spec.MetadataValidationInterval = metav1.Duration{Duration: 6 * time.Hour}
	br.Spec.SampledRetrievalInterval = &metav1.Duration{Duration: 6 * time.Hour}
	objects := int32(10)
	br.Spec.SampledRetrievalObjects = &objects
	return br
}

// finishedJob returns a Job of the given level that finished at the given
// time. owner may be nil to build a Job arete does not control.
func finishedJob(
	name, level string, owner *aretev1alpha1.BackupRepository, finished time.Time, succeeded bool,
) *batchv1.Job {
	j := &batchv1.Job{}
	j.Name = name
	j.Namespace = jobTestNamespace
	j.CreationTimestamp = metav1.NewTime(finished.Add(-3 * time.Minute))
	j.Labels = map[string]string{labelOwnerName: "tenant-data", labelLevel: level}
	if owner != nil {
		controller := true
		j.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: aretev1alpha1.GroupVersion.String(), Kind: "BackupRepository",
			Name: owner.Name, UID: owner.UID, Controller: &controller,
		}}
	}
	condType := batchv1.JobFailed
	if succeeded {
		condType = batchv1.JobComplete
		done := metav1.NewTime(finished)
		j.Status.CompletionTime = &done
	} else {
		// A failed Job has no completionTime; arete falls back to creation.
		j.CreationTimestamp = metav1.NewTime(finished)
	}
	j.Status.Conditions = []batchv1.JobCondition{{Type: condType, Status: corev1.ConditionTrue}}
	return j
}

func jobTestReconciler(t *testing.T, objs ...client.Object) *BackupRepositoryReconciler {
	t.Helper()
	s := jobTestScheme(t)
	return &BackupRepositoryReconciler{
		Client:          fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build(),
		Scheme:          s,
		ValidatorImages: ValidatorImages{Restic: "restic:test", Walg: "walg:test"},
	}
}

func countJobs(t *testing.T, r *BackupRepositoryReconciler) int {
	t.Helper()
	var jobs batchv1.JobList
	if err := r.List(context.Background(), &jobs, client.InNamespace(jobTestNamespace)); err != nil {
		t.Fatal(err)
	}
	return len(jobs.Items)
}

func TestProcessE2_RunsOncePerInterval(t *testing.T) {
	now := time.Now()

	t.Run("spawns when a run is due", func(t *testing.T) {
		br := jobTestRepository()
		br.Status.VerifiedLastValidationAt = mtime(now.Add(-7 * time.Hour))
		r := jobTestReconciler(t)

		out := r.processE2(context.Background(), br, time.Time{}, false, true)
		if !out.jobActive || countJobs(t, r) != 1 {
			t.Fatalf("expected one new Job, active=%v jobs=%d", out.jobActive, countJobs(t, r))
		}
	})

	t.Run("does not run again right after reading a result", func(t *testing.T) {
		// Status still shows the previous run (7h ago) because it is only
		// patched at the end of the reconcile. The Job that just finished
		// must count, or every validation runs twice.
		br := jobTestRepository()
		br.Status.VerifiedLastValidationAt = mtime(now.Add(-7 * time.Hour))
		r := jobTestReconciler(t, finishedJob("e2-done", labelLevelE2, br, now.Add(-2*time.Minute), true))

		out := r.processE2(context.Background(), br, time.Time{}, false, true)
		if out.completedResult == nil {
			t.Fatal("the finished Job was not read")
		}
		if n := countJobs(t, r); n != 1 {
			t.Fatalf("a second validation was started straight after the first: %d Jobs", n)
		}
	})

	t.Run("does not spawn while another validation runs", func(t *testing.T) {
		br := jobTestRepository()
		r := jobTestReconciler(t)
		out := r.processE2(context.Background(), br, time.Time{}, false, false)
		if out.jobActive || countJobs(t, r) != 0 {
			t.Fatalf("spawned although not allowed: jobs=%d", countJobs(t, r))
		}
	})

	t.Run("a force annotation is honoured once", func(t *testing.T) {
		forceTS := now.Add(-10 * time.Minute)
		br := jobTestRepository()
		br.Status.VerifiedLastValidationAt = mtime(now.Add(-time.Hour)) // before the annotation
		r := jobTestReconciler(t, finishedJob("e2-forced", labelLevelE2, br, now.Add(-2*time.Minute), true))

		r.processE2(context.Background(), br, forceTS, true, true)
		if n := countJobs(t, r); n != 1 {
			t.Fatalf("the forced run already finished after the annotation; got %d Jobs", n)
		}
	})

	t.Run("a force annotation newer than the last run starts one", func(t *testing.T) {
		forceTS := now.Add(-time.Minute)
		br := jobTestRepository()
		br.Status.VerifiedLastValidationAt = mtime(now.Add(-time.Hour))
		r := jobTestReconciler(t)

		r.processE2(context.Background(), br, forceTS, true, true)
		if n := countJobs(t, r); n != 1 {
			t.Fatalf("expected the forced run to start, got %d Jobs", n)
		}
	})
}

func TestValidationJobs_OnlyOwnedJobsCount(t *testing.T) {
	now := time.Now()
	br := jobTestRepository()
	br.Status.VerifiedLastValidationAt = mtime(now.Add(-time.Hour))
	// Same labels, but not controlled by this BackupRepository: created by
	// someone else in the namespace, or left over from a deleted object.
	r := jobTestReconciler(t, finishedJob("e2-foreign", labelLevelE2, nil, now.Add(-2*time.Minute), true))

	out := r.processE2(context.Background(), br, time.Time{}, false, true)
	if out.completedResult != nil {
		t.Fatal("a Job arete does not control was accepted as a validation result")
	}
	if busy, err := r.validationJobInFlight(context.Background(), br); err != nil || busy {
		t.Fatalf("busy=%v err=%v", busy, err)
	}
}

func TestValidationJobInFlight(t *testing.T) {
	br := jobTestRepository()
	running := finishedJob("e3-running", labelLevelE3, br, time.Now(), true)
	running.Status = batchv1.JobStatus{Active: 1}
	r := jobTestReconciler(t, running)

	busy, err := r.validationJobInFlight(context.Background(), br)
	if err != nil || !busy {
		t.Fatalf("a running E3 Job must block other levels: busy=%v err=%v", busy, err)
	}
}

func TestProcessE3_ReadsResultsEvenWhenItMayNotSpawn(t *testing.T) {
	now := time.Now()
	br := jobTestRepository()
	br.Status.VerifiedLastSampledRetrievalAt = mtime(now.Add(-7 * time.Hour))
	r := jobTestReconciler(t, finishedJob("e3-done", labelLevelE3, br, now.Add(-2*time.Minute), true))

	// allowSpawn=false: another level is running. The finished E3 Job is
	// deleted ten minutes after it ends, so it has to be read now.
	out := r.processE3(context.Background(), br, S3Credentials{}, time.Time{}, false, false)
	if out.completedResult == nil {
		t.Fatal("the finished E3 Job was not read while spawning was blocked")
	}
	if n := countJobs(t, r); n != 1 {
		t.Fatalf("spawned although not allowed: %d Jobs", n)
	}
}

func TestProcessE4_FailedRunWithoutStatsIsRecorded(t *testing.T) {
	now := time.Now()
	br := jobTestRepository()
	br.Spec.FullRetrievalInterval = &metav1.Duration{Duration: 24 * time.Hour}
	class, size := "fast", resource.MustParse("10Gi")
	br.Spec.FullRetrievalStorageClass, br.Spec.FullRetrievalPVCSize = &class, &size
	failed := finishedJob("e4-failed", labelLevelE4, br, now.Add(-2*time.Minute), false)
	r := jobTestReconciler(t, failed)

	// No pod logs are available, so there is no STATS line to parse.
	out := r.processE4(context.Background(), br, false, true)
	if out.completedResult == nil || out.completedResult.Status != metav1.ConditionFalse {
		t.Fatalf("expected a False result, got %+v", out.completedResult)
	}
	if out.stats == nil || out.stats.CompletedAt.IsZero() {
		t.Fatal("a failed run without statistics must still be recorded, or it is re-read and re-run forever")
	}
	if n := countJobs(t, r); n != 1 {
		t.Fatalf("the failed scheduled E4 was restarted at once: %d Jobs", n)
	}
}

func TestE3Command_MissingObjectCountIsAnError(t *testing.T) {
	br := jobTestRepository()
	br.Spec.SampledRetrievalObjects = nil
	r := jobTestReconciler(t)
	if _, _, _, err := r.e3CommandArgsEnv(context.Background(), br, S3Credentials{}); err == nil {
		t.Fatal("expected an error (and no panic) when the object count is missing")
	}
}

func envIndex(env []corev1.EnvVar, name string) int {
	last := -1
	for i, e := range env {
		if e.Name == name {
			last = i
		}
	}
	return last
}

func TestValidatorEnv(t *testing.T) {
	spec := jobTestRepository().Spec
	spec.S3.Prefix = "tenant/$(RESTIC_PASSWORD)"
	spec.S3.ExtraEnv = map[string]string{
		"WALG_COMPRESSION_METHOD": "lzo",
		"RESTIC_PASSWORD_COMMAND": "cat /var/run/secrets/kubernetes.io/serviceaccount/token",
		"RESTIC_REPOSITORY":       "s3:https://elsewhere.example.com/other/repo",
		"AWS_ACCESS_KEY_ID":       "literal-override",
		"NOTE":                    "$(AWS_SECRET_ACCESS_KEY)",
	}
	env := validatorEnv(spec)

	t.Run("references to other variables are neutralised", func(t *testing.T) {
		for _, e := range env {
			if e.ValueFrom == nil && strings.Contains(strings.ReplaceAll(e.Value, "$$(", ""), "$(") {
				t.Errorf("%s=%q still contains an expandable reference", e.Name, e.Value)
			}
		}
		repo := env[envIndex(env, "RESTIC_REPOSITORY")]
		if !strings.HasSuffix(repo.Value, "tenant/$$(RESTIC_PASSWORD)") {
			t.Errorf("RESTIC_REPOSITORY = %q", repo.Value)
		}
	})

	t.Run("credentials come last and are secret-backed", func(t *testing.T) {
		firstSecret := -1
		for i, e := range env {
			if e.ValueFrom != nil && firstSecret < 0 {
				firstSecret = i
			}
			if e.ValueFrom == nil && firstSecret >= 0 {
				t.Errorf("literal %s comes after a secret-backed variable", e.Name)
			}
		}
		// For duplicate names the last entry wins: the credential, not extraEnv.
		if last := env[envIndex(env, "AWS_ACCESS_KEY_ID")]; last.ValueFrom == nil {
			t.Error("extraEnv replaced a credential")
		}
	})

	t.Run("extraEnv cannot repoint the validator", func(t *testing.T) {
		repo := env[envIndex(env, "RESTIC_REPOSITORY")]
		if !strings.HasPrefix(repo.Value, "s3:https://s3.example.com/backups/") {
			t.Errorf("effective RESTIC_REPOSITORY = %q", repo.Value)
		}
	})

	t.Run("dangerous names are dropped, harmless ones pass", func(t *testing.T) {
		if envIndex(env, "RESTIC_PASSWORD_COMMAND") >= 0 {
			t.Error("RESTIC_PASSWORD_COMMAND reached the container")
		}
		if envIndex(env, "WALG_COMPRESSION_METHOD") < 0 {
			t.Error("WALG_COMPRESSION_METHOD was dropped")
		}
	})
}

func TestLatestCompletion(t *testing.T) {
	now := time.Now()
	br := jobTestRepository()
	older, newer := now.Add(-time.Hour), now.Add(-time.Minute)
	jobs := []batchv1.Job{*finishedJob("a", labelLevelE2, br, newer, true)}

	if got := latestCompletion(nil, nil); got != nil {
		t.Errorf("nothing known: got %v", got)
	}
	if got := latestCompletion(mtime(older), nil); !got.Time.Equal(older) {
		t.Errorf("no Jobs: got %v, want the recorded time", got)
	}
	if got := latestCompletion(mtime(older), jobs); got.Sub(newer).Abs() > time.Second {
		t.Errorf("a newer finished Job must win: got %v, want %v", got, newer)
	}
	if got := latestCompletion(mtime(now), jobs); got.Sub(now).Abs() > time.Second {
		t.Errorf("a newer recorded time must win: got %v, want %v", got, now)
	}
}

func TestForceTimestampInFuture(t *testing.T) {
	now := time.Now()
	for offset, want := range map[time.Duration]bool{
		-time.Hour: false, 0: false, 2 * time.Minute: false, // clock skew is tolerated
		10 * time.Minute: true, 365 * 24 * time.Hour: true,
	} {
		if got := forceTimestampInFuture(now.Add(offset), now); got != want {
			t.Errorf("offset %v: got %v, want %v", offset, got, want)
		}
	}
}

// TestE4Script_ExitStatusIsTheValidators runs the real E4 scripts with a
// stand-in for the validator binary. The retrieval is arete's strongest
// claim about a repository, so its script must fail when the tool fails.
func TestE4Script_ExitStatusIsTheValidators(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh available")
	}
	cases := []struct {
		format aretev1alpha1.BackupFormat
		binary string
	}{
		{aretev1alpha1.BackupFormatRestic, "restic"},
		{aretev1alpha1.BackupFormatWalg, "wal-g"},
	}
	for _, tc := range cases {
		for _, wantRC := range []int{0, 3} {
			name := string(tc.format) + " succeeds"
			if wantRC != 0 {
				name = string(tc.format) + " fails"
			}
			t.Run(name, func(t *testing.T) {
				work, bin := t.TempDir(), t.TempDir()
				// The stand-in prints more than ten lines (the script tails
				// ten) and, like wal-g backup-fetch, writes into the
				// directory it is given.
				stub := "#!/bin/sh\nfor i in 1 2 3 4 5 6 7 8 9 10 11 12; do echo \"line $i\"; done\n" +
					"[ \"$1\" = backup-fetch ] && echo restored > \"$2/PG_VERSION\"\n" +
					"exit ${STUB_RC}\n"
				if err := os.WriteFile(filepath.Join(bin, tc.binary), []byte(stub), 0o755); err != nil {
					t.Fatal(err)
				}
				script, err := e4Script(tc.format, work)
				if err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command(sh, "-c", script)
				cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"),
					"STUB_RC="+map[int]string{0: "0", 3: "3"}[wantRC])
				out, err := cmd.CombinedOutput()

				gotRC := 0
				if exitErr, ok := err.(*exec.ExitError); ok {
					gotRC = exitErr.ExitCode()
				} else if err != nil {
					t.Fatalf("run script: %v\n%s", err, out)
				}
				if gotRC != wantRC {
					t.Fatalf("script exited %d, the validator exited %d\n%s", gotRC, wantRC, out)
				}
				stats := parseE4Stats(string(out))
				if stats == nil {
					t.Fatalf("no STATS line in output:\n%s", out)
				}
				if wantFailed := int64(min(wantRC, 1)); stats.FailedObjects != wantFailed {
					t.Errorf("failed=%d, want %d\n%s", stats.FailedObjects, wantFailed, out)
				}
				if !strings.Contains(string(out), "line 12") || strings.Contains(string(out), "line 1\n") {
					t.Errorf("expected the last ten lines of the tool's output:\n%s", out)
				}
				if tc.format == aretev1alpha1.BackupFormatWalg {
					if stats.BytesTransferred == 0 {
						t.Errorf("restored bytes were not measured:\n%s", out)
					}
					if _, err := os.Stat(filepath.Join(work, "extract")); !os.IsNotExist(err) {
						t.Error("the restored data directory was left on the volume")
					}
				}
			})
		}
	}
}

// TestRepositoryControllerOptions pins two settings that decide whether every
// repository is probed on schedule. With controller-runtime's default
// priority queue and a single worker, repositories listed at start-up were
// never probed again after a restart while a few busy ones kept the worker.
func TestRepositoryControllerOptions(t *testing.T) {
	o := repositoryControllerOptions()
	if o.UsePriorityQueue == nil || *o.UsePriorityQueue {
		t.Error("the repository queue must be first-in-first-out, not the priority queue")
	}
	if o.MaxConcurrentReconciles < 2 {
		t.Errorf("MaxConcurrentReconciles = %d; one worker cannot keep up with slow probes", o.MaxConcurrentReconciles)
	}
}

func TestInventoryDue(t *testing.T) {
	now := time.Now()
	br := jobTestRepository() // metadataValidationInterval 6h
	br.Generation = 3
	r := &BackupRepositoryReconciler{}

	if !r.inventoryDue(br, now) {
		t.Fatal("first probe after a start must take the inventory")
	}

	r.recordInventory(br, now.Add(-10*time.Minute), true)
	if r.inventoryDue(br, now) {
		t.Error("ten minutes after a successful inventory it must not run again")
	}

	r.recordInventory(br, now.Add(-7*time.Hour), true)
	if !r.inventoryDue(br, now) {
		t.Error("after metadataValidationInterval the inventory is due again")
	}

	// A failed listing is retried, but not on every probe.
	r.recordInventory(br, now.Add(-10*time.Minute), false)
	if r.inventoryDue(br, now) {
		t.Error("a failed inventory must not be retried ten minutes later")
	}
	r.recordInventory(br, now.Add(-61*time.Minute), false)
	if !r.inventoryDue(br, now) {
		t.Error("a failed inventory is due again after the retry interval")
	}

	// A changed spec may mean a new location: its numbers must be fresh.
	r.recordInventory(br, now.Add(-time.Minute), true)
	br.Generation = 4
	if !r.inventoryDue(br, now) {
		t.Error("a spec change must make the inventory due")
	}

	// Another repository is unaffected.
	other := jobTestRepository()
	other.Name = "other"
	if !r.inventoryDue(other, now) {
		t.Error("each repository has its own schedule")
	}
}

func TestMapSecretToRepositories(t *testing.T) {
	br := jobTestRepository()
	other := jobTestRepository()
	other.Name, other.UID = "other-tenant", types.UID("22222222-2222-2222-2222-222222222222")
	other.Spec.S3.CredentialsSecret = aretev1alpha1.SecretReference{Name: "creds", Namespace: "elsewhere"}
	r := jobTestReconciler(t, br, other)

	// The watch delivers metadata only; the mapping must work without the
	// Secret's contents.
	meta := &metav1.PartialObjectMetadata{}
	meta.Name, meta.Namespace = "creds", jobTestNamespace

	got := r.mapSecretToRepositories(context.Background(), meta)
	if len(got) != 1 || got[0].Name != br.Name {
		t.Fatalf("expected a request for %q only, got %v", br.Name, got)
	}

	meta.Name = "unrelated"
	if got := r.mapSecretToRepositories(context.Background(), meta); len(got) != 0 {
		t.Fatalf("an unrelated Secret must not trigger anything, got %v", got)
	}
}
