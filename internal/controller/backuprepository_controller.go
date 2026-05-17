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
	"fmt"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aretev1alpha1 "github.com/TobiasHofmaenner/arete/api/v1alpha1"
	aretemetrics "github.com/TobiasHofmaenner/arete/internal/metrics"
)

// ErrCredentialsSecretNotFound is the sentinel returned by
// resolveCredentials when the referenced Secret does not exist. The
// Reconcile loop treats this case specially: a brief gap (e.g. during a
// DR drill where the tenant namespace is being recreated) is tolerated
// for credentialsTransientTolerance before any condition flips.
var ErrCredentialsSecretNotFound = errors.New("credentials secret not found")

// credentialsTransientTolerance bounds how long we wait for a missing
// Secret to reappear before declaring the BR un-Reachable. Exists so a
// DR drill that briefly destroys the tenant namespace doesn't take down
// the BR (and via the BRC, all dependent resources). 60s is roughly an
// order of magnitude longer than the typical Flux re-create window
// (5-15s) but well below the fastest valid recovery interval. Status
// conditions are NOT touched within this window — the BR shows last
// known-good values.
const credentialsTransientTolerance = 60 * time.Second

// credentialsTransientRequeue is the requeue interval used while we're
// waiting for the Secret to reappear. Short enough that recovery is
// quick, long enough to avoid hot-looping.
const credentialsTransientRequeue = 30 * time.Second

// ValidatorImages holds the per-format validator container image
// references the controller passes to Jobs it spawns for E2-E4. Pinned per
// arete release via the Helm chart (see ADR-023's always-latest-pinned
// strategy + forward-compat-canary insight).
type ValidatorImages struct {
	Walg   string
	Restic string
}

// BackupRepositoryReconciler reconciles a BackupRepository object
type BackupRepositoryReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	ValidatorImages ValidatorImages
	// PodLogs streams logs from a pod (injected at startup to keep the
	// controller package free of client-go imports). Optional; when nil,
	// MetadataValid messages omit the validator's stderr.
	PodLogs PodLogStreamer

	// credsTransient tracks when each BR first saw its credentials
	// Secret missing. Populated on first NotFound, cleared on successful
	// resolveCredentials. Lives in memory: lost on controller restart,
	// which is acceptable — restart already triggers a full re-reconcile
	// of every BR. Keyed by NamespacedName, value is time.Time.
	credsTransient sync.Map
}

// +kubebuilder:rbac:groups=arete.arete.io,resources=backuprepositories,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=arete.arete.io,resources=backuprepositories/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=arete.arete.io,resources=backuprepositories/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;delete

func (r *BackupRepositoryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var br aretev1alpha1.BackupRepository
	if err := r.Get(ctx, req.NamespacedName, &br); err != nil {
		// BR deleted: drop any in-memory state for it.
		r.credsTransient.Delete(req.NamespacedName)
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	creds, err := r.resolveCredentials(ctx, &br)
	if err != nil {
		// Special-case "Secret transiently missing" so a brief gap (a
		// few seconds during a DR drill where the tenant namespace is
		// being torn down + recreated) doesn't flip the BR's status to
		// degraded — which would in turn make every dependent BRC flip
		// its decision and tear apart the recovery path itself. Any
		// other credential error (forbidden, malformed, missing keys)
		// is reported immediately.
		if errors.Is(err, ErrCredentialsSecretNotFound) {
			firstSeen, _ := r.credsTransient.LoadOrStore(req.NamespacedName, time.Now())
			elapsed := time.Since(firstSeen.(time.Time))
			if elapsed < credentialsTransientTolerance {
				log.Info("credentials transiently unavailable; preserving status",
					"elapsed", elapsed,
					"tolerance", credentialsTransientTolerance)
				return ctrl.Result{RequeueAfter: credentialsTransientRequeue}, nil
			}
			log.Info("credentials missing beyond tolerance; flipping status",
				"elapsed", elapsed, "err", err)
		}
		return r.recordCredentialsFailure(ctx, &br, err)
	}

	// Credentials OK — drop any tolerance counter for this BR.
	r.credsTransient.Delete(req.NamespacedName)

	// Resolve operator-driven force-revalidate. The `force` flag is
	// then narrowed per level: each level honors the annotation
	// exactly once (via the verifiedLast{Validation,SampledRetrieval}At
	// > forceTS check below), so a single annotation walks E2 → E3
	// across reconciles rather than firing the same level repeatedly.
	forceTS, force := r.resolveForceRevalidate(&br)

	// Per-level force gates. e2/e3 ForceNeeded is true when the
	// operator wants this level re-validated AND that hasn't yet
	// happened post-annotation. Avoids re-firing levels every cycle
	// while the broader force annotation is still "live" (i.e.,
	// before applyStatus has bumped status.lastForceRevalidatedAt).
	var e2ForceNeeded, e3ForceNeeded bool
	if force {
		e2ForceNeeded = !levelVerifiedAfter(br.Status.VerifiedLastValidationAt, forceTS)
		if br.Spec.SampledRetrievalInterval != nil {
			e3ForceNeeded = !levelVerifiedAfter(br.Status.VerifiedLastSampledRetrievalAt, forceTS)
		}
	}

	// Resolve operator-driven force-e4. Independent annotation from
	// force-revalidate — E4 is heavy and on-demand-only by design, so
	// it's controlled separately rather than tagging along with E2/E3.
	forceE4TS, forceE4 := r.resolveForceE4(&br)

	// E4 hasn't been verified post-annotation iff LastFullRetrieval is
	// nil OR its CompletedAt is not strictly after forceE4TS.
	var e4ForceNeeded bool
	if forceE4 {
		alreadyPostForce := br.Status.LastFullRetrieval != nil &&
			br.Status.LastFullRetrieval.CompletedAt.After(forceE4TS)
		e4ForceNeeded = !alreadyPostForce
	}

	// E1 probe (in-process; cheap)
	probe := probeRepository(ctx, br.Spec, creds)

	// E2 Job lifecycle: process any completed Job, maybe spawn a new one
	e2 := r.processE2(ctx, &br, e2ForceNeeded)

	// E3 Job lifecycle: opt-in via spec.sampledRetrievalInterval.
	// Sequenced after E2 — restic's `check` (E3) takes an exclusive
	// repository lock that contends with `snapshots` (E2)'s shared
	// lock attempt. Naturally rare at default cadence (E2 hourly, E3
	// 6-hourly) but a force-revalidate annotation fires both at once
	// and produces a deterministic deadlock on restic repos. Skip E3
	// while an E2 Job is in flight; it'll fire on the next reconcile.
	var e3 e3Outcome
	if !e2.jobActive {
		e3 = r.processE3(ctx, &br, creds, e3ForceNeeded)
	}

	// E4 Job lifecycle: opt-in via spec.fullRetrievalInterval (schedule)
	// OR the arete.io/force-e4 annotation (on-demand). Same sequencing
	// rationale: full retrieval also exclusive-locks the restic repo,
	// so skip while either E2 or E3 is in flight.
	var e4 e4Outcome
	if !e2.jobActive && !e3.jobActive {
		e4 = r.processE4(ctx, &br, e4ForceNeeded)
	}

	// Honor the force annotation in status once every forced level
	// has produced a verified-after-force result. This is a strict
	// post-completion check: E2 must have a verifiedLastValidationAt
	// strictly after forceTS, and E3 (when enabled) likewise. The
	// honored timestamp lives in status.lastForceRevalidatedAt so
	// the same annotation value won't re-trigger on later cycles.
	var honoredForceAt *metav1.Time
	if force && !e2ForceNeeded && !e3ForceNeeded {
		t := metav1.NewTime(forceTS)
		honoredForceAt = &t
	}

	// Same idempotency for force-e4: honor once the post-force E4
	// has actually completed (signaled by !e4ForceNeeded after this
	// reconcile's processE4 ingested the Job result).
	var honoredForceE4At *metav1.Time
	if forceE4 && !e4ForceNeeded {
		t := metav1.NewTime(forceE4TS)
		honoredForceE4At = &t
	}

	if err := r.applyStatus(ctx, &br, probe, e2, e3, e4, honoredForceAt, honoredForceE4At); err != nil {
		log.Error(err, "failed to update status")
		return ctrl.Result{}, err
	}

	publishMetrics(&br)

	return ctrl.Result{RequeueAfter: br.Spec.ProbeInterval.Duration}, nil
}

// levelVerifiedAfter reports whether the given verifiedLast* timestamp
// is set AND strictly after the reference time. Used by the
// force-revalidate flow to determine whether a level still needs
// to fire under the current annotation, or whether its post-annotation
// run has already completed.
func levelVerifiedAfter(verified *metav1.Time, ref time.Time) bool {
	return verified != nil && verified.After(ref)
}

// resolveForceRevalidate parses the `arete.io/force-revalidate`
// annotation. Returns (parsed timestamp, true) if the annotation is
// fresher than status.lastForceRevalidatedAt; otherwise (zero, false).
// Malformed timestamps are ignored (logged but not surfaced as a
// condition — the contract is "honor or skip", never "block").
func (r *BackupRepositoryReconciler) resolveForceRevalidate(
	br *aretev1alpha1.BackupRepository,
) (time.Time, bool) {
	ann := br.GetAnnotations()[aretev1alpha1.AnnotationForceRevalidate]
	if ann == "" {
		return time.Time{}, false
	}
	ts, err := time.Parse(time.RFC3339, ann)
	if err != nil {
		// Try RFC3339Nano too (kubectl annotate with $(date) emits this).
		ts, err = time.Parse(time.RFC3339Nano, ann)
		if err != nil {
			return time.Time{}, false
		}
	}
	if br.Status.LastForceRevalidatedAt != nil &&
		!ts.After(br.Status.LastForceRevalidatedAt.Time) {
		return time.Time{}, false
	}
	return ts, true
}

// clearForceE4Annotation removes the arete.io/force-e4 annotation
// from the BR after the corresponding Job has been spawned. Best
// effort: failure is logged at the call site but doesn't abort the
// reconcile (the annotation will linger and be re-honored on the
// next cycle — at worst a duplicate Job runs).
func (r *BackupRepositoryReconciler) clearForceE4Annotation(
	ctx context.Context, br *aretev1alpha1.BackupRepository,
) error {
	if _, ok := br.GetAnnotations()[aretev1alpha1.AnnotationForceE4]; !ok {
		return nil
	}
	// JSON merge patch: setting the annotation key to null deletes it.
	patch := []byte(`{"metadata":{"annotations":{"` + aretev1alpha1.AnnotationForceE4 + `":null}}}`)
	return r.Patch(ctx, br, client.RawPatch(types.MergePatchType, patch))
}

// resolveForceE4 parses the `arete.io/force-e4` annotation. Returns
// (parsed timestamp, true) if the annotation is fresher than
// status.lastForcedE4At; otherwise (zero, false). Mirror of
// resolveForceRevalidate — same idempotency contract: honor once
// per unique annotation value, never re-loop.
func (r *BackupRepositoryReconciler) resolveForceE4(
	br *aretev1alpha1.BackupRepository,
) (time.Time, bool) {
	ann := br.GetAnnotations()[aretev1alpha1.AnnotationForceE4]
	if ann == "" {
		return time.Time{}, false
	}
	ts, err := time.Parse(time.RFC3339, ann)
	if err != nil {
		ts, err = time.Parse(time.RFC3339Nano, ann)
		if err != nil {
			return time.Time{}, false
		}
	}
	if br.Status.LastForcedE4At != nil &&
		!ts.After(br.Status.LastForcedE4At.Time) {
		return time.Time{}, false
	}
	return ts, true
}

// publishMetrics snapshots the BackupRepository's status into the
// per-BR Prometheus gauges. Called every reconcile so freshness alerts
// (BackupAgeSeconds, ConditionState) reflect current state. Counters
// and histograms are bumped separately on ingest (recordValidationRun).
func publishMetrics(br *aretev1alpha1.BackupRepository) {
	format := string(br.Spec.Format)

	if br.Status.ClaimedLastSuccessfulBackup != nil {
		age := time.Since(br.Status.ClaimedLastSuccessfulBackup.Time).Seconds()
		aretemetrics.BackupAgeSeconds.WithLabelValues(br.Name, format).Set(age)
	}

	if e4 := br.Status.LastFullRetrieval; e4 != nil {
		aretemetrics.E4ThroughputBPS.WithLabelValues(br.Name, format).Set(float64(e4.ThroughputBytesPerSec))
		aretemetrics.E4BytesTransferred.WithLabelValues(br.Name, format).Set(float64(e4.BytesTransferred))
	}

	if inv := br.Status.ObservedInventory; inv != nil {
		aretemetrics.InventoryObjects.WithLabelValues(br.Name, format).Set(float64(inv.ObjectCount))
		if v, ok := inv.TotalBytes.AsInt64(); ok {
			aretemetrics.InventoryBytes.WithLabelValues(br.Name, format).Set(float64(v))
		}
	}

	for _, c := range br.Status.Conditions {
		var v float64
		switch c.Status {
		case metav1.ConditionTrue:
			v = 1
		case metav1.ConditionFalse:
			v = 0
		case metav1.ConditionUnknown:
			v = -1
		}
		aretemetrics.ConditionState.WithLabelValues(br.Name, format, c.Type).Set(v)
	}
}

// recordValidationRun bumps the run counter + duration histogram for a
// finished E2/E3/E4 Job. Called from each ingestE*Result.
func recordValidationRun(br *aretev1alpha1.BackupRepository, level string, job *batchv1.Job) {
	format := string(br.Spec.Format)
	result := "success"
	if !jobSucceeded(job) {
		result = "failure"
	}
	aretemetrics.ValidationRunsTotal.WithLabelValues(br.Name, format, level, result).Inc()

	if job.Status.StartTime != nil {
		duration := jobCompletionTime(job).Sub(job.Status.StartTime.Time).Seconds()
		if duration > 0 {
			aretemetrics.ValidationDurationSeconds.WithLabelValues(br.Name, format, level).Observe(duration)
		}
	}
}

// e2Outcome captures what we learned about E2 this reconcile cycle:
// whether a Job is currently running, and the result of the most recent
// completed Job (if newer than verifiedLastValidationAt).
type e2Outcome struct {
	jobActive       bool
	completedResult *metav1.Condition                 // nil if no new completed job to process
	completedAt     *time.Time                        // nil if completedResult is nil
	latestBackup    *aretev1alpha1.LatestBackupStatus // nil if validator didn't emit JSON or parse failed
}

// e3Outcome mirrors e2Outcome for the sampled retrieval level.
// LatestSampleAt is the controller's running record of "when did the
// last E3 run finish" — distinct from VerifiedLastValidationAt (E2's
// scope). Lives in status.verifiedLastSampledRetrievalAt.
type e3Outcome struct {
	jobActive       bool
	completedResult *metav1.Condition
	completedAt     *time.Time
}

// e4Outcome mirrors e3Outcome for the full retrieval level. The
// timestamp lives in status.lastFullRetrieval.completedAt — there's no
// separate verifiedLastFullRetrievalAt because LastFullRetrieval is
// itself a richer struct that covers the same role.
type e4Outcome struct {
	jobActive       bool
	completedResult *metav1.Condition
	stats           *aretev1alpha1.FullRetrievalStatus
}

// processE2 inspects existing E2 Jobs for this BR, ingests results from
// any newly-completed Job, and spawns a fresh Job when due. Pass
// force=true to bypass the metadataValidationInterval cooldown (used by
// the `arete.io/force-revalidate` annotation path).
func (r *BackupRepositoryReconciler) processE2(
	ctx context.Context, br *aretev1alpha1.BackupRepository, force bool,
) e2Outcome {
	log := logf.FromContext(ctx)
	out := e2Outcome{}

	jobs, err := r.listE2Jobs(ctx, br)
	if err != nil {
		log.Error(err, "list e2 jobs")
		return out
	}

	if active := firstActiveJob(jobs); active != nil {
		out.jobActive = true
	}

	// If a completed Job is newer than what we last recorded, ingest it.
	if latest := pickLatestCompletedJob(jobs); latest != nil {
		completedAt := jobCompletionTime(latest)
		alreadyIngested := br.Status.VerifiedLastValidationAt != nil &&
			!completedAt.After(br.Status.VerifiedLastValidationAt.Time)
		if !alreadyIngested {
			cond, latestBackup := r.ingestE2Result(ctx, br, latest)
			out.completedResult = cond
			out.completedAt = &completedAt
			out.latestBackup = latestBackup
			recordValidationRun(br, "e2", latest)
		}
	}

	// Spawn a new Job if due and none currently running.
	if !out.jobActive && (force || shouldSpawnE2(br, time.Now())) {
		if _, err := r.spawnE2Job(ctx, br); err != nil {
			log.Error(err, "spawn e2 job")
		} else {
			out.jobActive = true
			log.Info("spawned E2 job", "format", br.Spec.Format, "force", force)
		}
	}

	return out
}

// processE3 mirrors processE2 for sampled retrieval. Skipped entirely
// when the level is not enabled (sampledRetrievalInterval nil) — the
// SampledIntegrityValid condition stays absent or stale-True from the
// previous run.
func (r *BackupRepositoryReconciler) processE3(
	ctx context.Context, br *aretev1alpha1.BackupRepository, creds S3Credentials, force bool,
) e3Outcome {
	log := logf.FromContext(ctx)
	out := e3Outcome{}
	if br.Spec.SampledRetrievalInterval == nil {
		return out
	}

	jobs, err := r.listE3Jobs(ctx, br)
	if err != nil {
		log.Error(err, "list e3 jobs")
		return out
	}

	if active := firstActiveJob(jobs); active != nil {
		out.jobActive = true
	}

	if latest := pickLatestCompletedJob(jobs); latest != nil {
		completedAt := jobCompletionTime(latest)
		alreadyIngested := br.Status.VerifiedLastSampledRetrievalAt != nil &&
			!completedAt.After(br.Status.VerifiedLastSampledRetrievalAt.Time)
		if !alreadyIngested {
			out.completedResult = r.ingestE3Result(ctx, latest)
			out.completedAt = &completedAt
			recordValidationRun(br, "e3", latest)
		}
	}

	if !out.jobActive && (force || shouldSpawnE3(br, br.Status.VerifiedLastSampledRetrievalAt, time.Now())) {
		if _, err := r.spawnE3Job(ctx, br, creds); err != nil {
			log.Error(err, "spawn e3 job")
		} else {
			out.jobActive = true
			log.Info("spawned E3 job", "format", br.Spec.Format, "force", force)
		}
	}

	return out
}

// ingestE3Result turns a finished E3 Job into a SampledIntegrityValid
// condition. Exit 0 → True; otherwise False with the last log line.
func (r *BackupRepositoryReconciler) ingestE3Result(
	ctx context.Context, job *batchv1.Job,
) *metav1.Condition {
	logs := r.readJobOutput(ctx, job)
	if !jobSucceeded(job) {
		return condFalse(
			aretev1alpha1.ReasonSampledIntegrityFailed,
			fmt.Sprintf("sampled retrieval failed; last log: %s", lastLine(logs)),
		)
	}
	return condTrue(
		aretev1alpha1.ReasonProbeSucceeded,
		fmt.Sprintf("sampled retrieval ok; last log: %s", lastLine(logs)),
	)
}

// processE4 mirrors processE3 for full retrieval. Skipped entirely when
// fullRetrievalInterval is nil. Uses LastFullRetrieval.CompletedAt as
// both the "we already ingested this Job" marker and the "due for
// another run" anchor — no separate VerifiedLastFullRetrievalAt field.
func (r *BackupRepositoryReconciler) processE4(
	ctx context.Context, br *aretev1alpha1.BackupRepository, forceNeeded bool,
) e4Outcome {
	log := logf.FromContext(ctx)
	out := e4Outcome{}

	jobs, err := r.listE4Jobs(ctx, br)
	if err != nil {
		log.Error(err, "list e4 jobs")
		return out
	}

	if active := firstActiveJob(jobs); active != nil {
		out.jobActive = true
	}

	// Ingest any completed Job result unconditionally — even if neither
	// schedule nor force is active right now, a Job that completed under
	// a previous force annotation (now cleared) still needs its result
	// recorded. Without this, a completed E4 from a force trigger that
	// got its annotation pruned by Flux SSA between spawn and ingest
	// would never make it into status.lastFullRetrieval.
	if latest := pickLatestCompletedJob(jobs); latest != nil {
		completedAt := jobCompletionTime(latest)
		alreadyIngested := br.Status.LastFullRetrieval != nil &&
			!completedAt.After(br.Status.LastFullRetrieval.CompletedAt.Time)
		if !alreadyIngested {
			cond, stats := r.ingestE4Result(ctx, latest)
			out.completedResult = cond
			if stats != nil {
				stats.CompletedAt = metav1.NewTime(completedAt)
			}
			out.stats = stats
			recordValidationRun(br, "e4", latest)
		}
	}

	// Spawn gate: only spawn a new Job when E4 is enabled — either via
	// schedule (interval set) or via active force annotation.
	if br.Spec.FullRetrievalInterval == nil && !forceNeeded {
		return out
	}

	var lastE4 *metav1.Time
	if br.Status.LastFullRetrieval != nil {
		lastE4 = &br.Status.LastFullRetrieval.CompletedAt
	}
	scheduledDue := shouldSpawnE4(br, lastE4, time.Now())
	if !out.jobActive && (scheduledDue || forceNeeded) {
		// Spawn-time spec validation. SC + PVCSize aren't required by the
		// CRD (they're +optional) because they're only meaningful when E4
		// actually runs. Refuse loudly here if E4 was requested but the
		// destination is unspecified — preserves the strict contract; the
		// annotation stays in place and will retry next reconcile once
		// the operator fixes the spec.
		if br.Spec.FullRetrievalStorageClass == nil || br.Spec.FullRetrievalPVCSize == nil {
			log.Error(nil, "E4 requested but spec is incomplete",
				"forceNeeded", forceNeeded,
				"scheduledDue", scheduledDue,
				"missingStorageClass", br.Spec.FullRetrievalStorageClass == nil,
				"missingPVCSize", br.Spec.FullRetrievalPVCSize == nil)
			return out
		}
		if _, err := r.spawnE4Job(ctx, br, forceNeeded); err != nil {
			log.Error(err, "spawn e4 job")
		} else {
			out.jobActive = true
			log.Info("spawned E4 job", "format", br.Spec.Format, "forced", forceNeeded)
			// Forced spawns: clear the annotation on the BR right after
			// the Job exists. Flux SSA's metadata reconciliation tends
			// to drop the annotation between spawn and ingest otherwise
			// (caught 2026-05-17 — long-running E4 + 10m Flux interval
			// = annotation cleared mid-Job, status.lastForcedE4At never
			// updated). The Job's trigger-source label preserves the
			// 'this was forced' fact for the eventual ingest.
			if forceNeeded {
				if err := r.clearForceE4Annotation(ctx, br); err != nil {
					log.Error(err, "clear force-e4 annotation")
				}
			}
		}
	}

	return out
}

// ingestE4Result turns a finished E4 Job into a (FullRetrievalCompleted
// condition, FullRetrievalStatus) pair. The condition reflects exit
// code; the stats come from the validator's STATS line. On failure we
// still try to parse stats — partial timing tells the user how far the
// retrieval got before bailing.
func (r *BackupRepositoryReconciler) ingestE4Result(
	ctx context.Context, job *batchv1.Job,
) (*metav1.Condition, *aretev1alpha1.FullRetrievalStatus) {
	logs := r.readJobOutput(ctx, job)
	stats := parseE4Stats(logs)
	// Restore the trigger source from the Job's label — parseE4Stats
	// can't know what triggered the spawn; spawnE4Job set the label
	// based on the forceNeeded flag at spawn time.
	if stats != nil {
		switch job.Labels[labelTriggerSource] {
		case string(aretev1alpha1.TriggerSourceManual):
			stats.TriggerSource = aretev1alpha1.TriggerSourceManual
		default:
			stats.TriggerSource = aretev1alpha1.TriggerSourceScheduled
		}
	}
	if !jobSucceeded(job) {
		return condFalse(
			aretev1alpha1.ReasonFullRetrievalFailed,
			fmt.Sprintf("full retrieval failed; last log: %s", lastLine(logs)),
		), stats
	}
	return condTrue(
		aretev1alpha1.ReasonProbeSucceeded,
		fmt.Sprintf("full retrieval ok; last log: %s", lastLine(logs)),
	), stats
}

// ingestE2Result turns a finished Job into a (MetadataValid condition,
// claimedLatestBackup struct). The condition reflects exit code; the
// LatestBackup is parsed from the validator's JSON output if present.
// On failure, both reflect the diagnostic last log line.
func (r *BackupRepositoryReconciler) ingestE2Result(
	ctx context.Context, br *aretev1alpha1.BackupRepository, job *batchv1.Job,
) (*metav1.Condition, *aretev1alpha1.LatestBackupStatus) {
	logs := r.readJobOutput(ctx, job)
	if !jobSucceeded(job) {
		return condFalse(
			aretev1alpha1.ReasonMetadataValidationFailed,
			fmt.Sprintf("validator failed; last log: %s", lastLine(logs)),
		), nil
	}
	latestBackup := parseE2Output(br.Spec.Format, logs)
	return condTrue(
		aretev1alpha1.ReasonProbeSucceeded,
		fmt.Sprintf("validator exit 0; last log: %s", lastLine(logs)),
	), latestBackup
}

// lastLine extracts the last non-empty line from multi-line output.
// Used to keep the condition message short while still informative.
func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		l := strings.TrimSpace(lines[i])
		if l != "" {
			if len(l) > 200 {
				return l[:200] + "..."
			}
			return l
		}
	}
	return "(no output)"
}

// resolveCredentials reads the cross-namespace Secret referenced by
// spec.s3.credentialsSecret. Returns a clear error if the Secret is missing
// or required keys are absent — the strict contract demands a loud failure,
// not a silent fallback.
func (r *BackupRepositoryReconciler) resolveCredentials(
	ctx context.Context, br *aretev1alpha1.BackupRepository,
) (S3Credentials, error) {
	var secret corev1.Secret
	key := types.NamespacedName{
		Namespace: br.Spec.S3.CredentialsSecret.Namespace,
		Name:      br.Spec.S3.CredentialsSecret.Name,
	}
	if err := r.Get(ctx, key, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return S3Credentials{}, fmt.Errorf("%w: %s", ErrCredentialsSecretNotFound, key)
		}
		return S3Credentials{}, fmt.Errorf("get secret %s: %w", key, err)
	}

	mapping := br.Spec.S3.CredentialsSecret.KeyMapping
	access := string(secret.Data[secretKeyFor("AWS_ACCESS_KEY_ID", mapping)])
	secretKey := string(secret.Data[secretKeyFor("AWS_SECRET_ACCESS_KEY", mapping)])
	if access == "" || secretKey == "" {
		return S3Credentials{}, fmt.Errorf(
			"secret %s missing data for AWS_ACCESS_KEY_ID and/or AWS_SECRET_ACCESS_KEY (after keyMapping)",
			key,
		)
	}
	return S3Credentials{
		AccessKeyID:     access,
		SecretAccessKey: secretKey,
		SessionToken:    string(secret.Data[secretKeyFor("AWS_SESSION_TOKEN", mapping)]),
	}, nil
}

// secretKeyFor resolves a canonical env-var name to the actual key in
// the user's Secret. Returns the canonical name itself if no override
// is provided (identity mapping).
func secretKeyFor(canonical string, mapping map[string]string) string {
	if remapped, ok := mapping[canonical]; ok && remapped != "" {
		return remapped
	}
	return canonical
}

// recordCredentialsFailure handles the pre-probe failure case (no creds,
// so we can't reach S3 at all). Sets Reachable=False with a clear reason
// but preserves prior E2/E3/E4 conditions: a credential gap is an
// observability problem, not evidence that the data on the other side
// has rotted. When credentials return and validation runs, those
// preserved conditions get refreshed through the normal path.
func (r *BackupRepositoryReconciler) recordCredentialsFailure(
	ctx context.Context, br *aretev1alpha1.BackupRepository, credErr error,
) (ctrl.Result, error) {
	patch := client.MergeFrom(br.DeepCopy())
	now := metav1.Now()
	br.Status.LastProbedAt = &now
	br.Status.ObservedGeneration = br.Generation

	r.applyConditions(br, conditionInputs{
		reachable:              condFalse(aretev1alpha1.ReasonCredentialsUnavailable, credErr.Error()),
		metadataValid:          preservedCondition(br, aretev1alpha1.ConditionMetadataValid),
		sampledIntegrityValid:  preservedCondition(br, aretev1alpha1.ConditionSampledIntegrityValid),
		fullRetrievalCompleted: preservedCondition(br, aretev1alpha1.ConditionFullRetrievalCompleted),
	})

	if err := r.Status().Patch(ctx, br, patch); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: br.Spec.ProbeInterval.Duration}, nil
}

// preservedCondition returns a copy of the named condition if it
// represents real (i.e., not LayerTwoNotYetAvailable) state, else nil.
// Used by recordCredentialsFailure to keep E2/E3/E4 visible across an
// observability gap rather than blanking them to Unknown.
func preservedCondition(br *aretev1alpha1.BackupRepository, conditionType string) *metav1.Condition {
	existing := apimeta.FindStatusCondition(br.Status.Conditions, conditionType)
	if existing == nil || existing.Reason == aretev1alpha1.ReasonLayerTwoNotYetAvailable {
		return nil
	}
	c := *existing
	return &c
}

// applyStatus computes every condition and updates the structured status
// fields from the probe + e2 + e3 + e4 outcomes, then patches the
// BackupRepository. honoredForceAt / honoredForceE4At, when non-nil,
// are the parsed annotation values that this cycle just honored —
// recorded in status so the same annotation value won't loop. Must
// be applied after the MergeFrom snapshot below.
func (r *BackupRepositoryReconciler) applyStatus(
	ctx context.Context, br *aretev1alpha1.BackupRepository,
	p probeResult, e2 e2Outcome, e3 e3Outcome, e4 e4Outcome,
	honoredForceAt *metav1.Time,
	honoredForceE4At *metav1.Time,
) error {
	patch := client.MergeFrom(br.DeepCopy())
	now := metav1.Now()

	br.Status.LastProbedAt = &now
	br.Status.ObservedGeneration = br.Generation
	if honoredForceAt != nil {
		br.Status.LastForceRevalidatedAt = honoredForceAt
	}
	if honoredForceE4At != nil {
		br.Status.LastForcedE4At = honoredForceE4At
	}
	br.Status.DetectedFormat = p.DetectedFormat
	br.Status.DetectedVersion = p.DetectedVersion
	br.Status.ClaimedLastSuccessfulBackup = p.LastSuccessfulBackup

	// Per-format ownership of claimedLatestBackup:
	//   - wal-g: E1 owns it (sentinels are plaintext). probe.LatestBackup
	//     is non-nil for wal-g — write through every cycle.
	//   - restic: E2 owns it. probe.LatestBackup is nil for restic; E2's
	//     Job result populates it. Keep stale value on cycles without a
	//     new E2 result so the dashboard doesn't blink to nil.
	if p.LatestBackup != nil {
		br.Status.ClaimedLatestBackup = p.LatestBackup
	}
	if p.Inventory != nil {
		br.Status.ObservedInventory = p.Inventory
	}

	// Ingest any newly-completed E2 result into status.
	var metadataValid *metav1.Condition
	if e2.completedResult != nil {
		metadataValid = e2.completedResult
		t := metav1.NewTime(*e2.completedAt)
		br.Status.VerifiedLastValidationAt = &t
		// E2-owned formats (restic) overwrite claimedLatestBackup from
		// the validator output. E1-owned formats (wal-g) leave E2's nil
		// alone — the E1 write above already set the field.
		if e2.latestBackup != nil {
			br.Status.ClaimedLatestBackup = e2.latestBackup
		}
	} else if br.Status.VerifiedLastValidationAt != nil {
		// No new result this cycle — preserve previous condition.
		if existing := apimeta.FindStatusCondition(br.Status.Conditions,
			aretev1alpha1.ConditionMetadataValid); existing != nil &&
			existing.Reason != aretev1alpha1.ReasonLayerTwoNotYetAvailable {
			c := *existing
			metadataValid = &c
		} else {
			// Previous validation context is no longer authoritative:
			// the existing condition is missing or already shows
			// LayerTwoNotYetAvailable (e.g., reset by an unreachable
			// cycle when the credentials Secret briefly disappeared).
			// Clear the timestamp so shouldSpawnE2 fires on the next
			// reconcile instead of trusting stale freshness.
			br.Status.VerifiedLastValidationAt = nil
		}
	}

	// Ingest E3 result.
	var sampledIntegrityValid *metav1.Condition
	if e3.completedResult != nil {
		sampledIntegrityValid = e3.completedResult
		t := metav1.NewTime(*e3.completedAt)
		br.Status.VerifiedLastSampledRetrievalAt = &t
	} else if br.Status.VerifiedLastSampledRetrievalAt != nil {
		if existing := apimeta.FindStatusCondition(br.Status.Conditions,
			aretev1alpha1.ConditionSampledIntegrityValid); existing != nil &&
			existing.Reason != aretev1alpha1.ReasonLayerTwoNotYetAvailable {
			c := *existing
			sampledIntegrityValid = &c
		} else {
			// Mirror E2's logic: condition has been reset to
			// LayerTwoNotYetAvailable so the previous timestamp is
			// no longer meaningful — clear it to force a fresh E3
			// run on the next reconcile.
			br.Status.VerifiedLastSampledRetrievalAt = nil
		}
	}

	// Ingest E4 result. LastFullRetrieval is the timing source-of-truth.
	var fullRetrievalCompleted *metav1.Condition
	if e4.completedResult != nil {
		fullRetrievalCompleted = e4.completedResult
		if e4.stats != nil {
			br.Status.LastFullRetrieval = e4.stats
		}
	} else if br.Status.LastFullRetrieval != nil {
		if existing := apimeta.FindStatusCondition(br.Status.Conditions,
			aretev1alpha1.ConditionFullRetrievalCompleted); existing != nil &&
			existing.Reason != aretev1alpha1.ReasonLayerTwoNotYetAvailable {
			c := *existing
			fullRetrievalCompleted = &c
		} else {
			// Mirror E2/E3: existing condition is no longer
			// authoritative, so the LastFullRetrieval struct (which
			// shouldSpawnE4 uses as its time anchor via CompletedAt)
			// is cleared to force a fresh run on next reconcile.
			br.Status.LastFullRetrieval = nil
		}
	}

	r.applyConditions(br, conditionInputs{
		reachable: condFromBool(p.Reachable, p.ReachableReason, p.ReachableMessage),
		bucketSecurityValid: ifReachable(p.Reachable,
			condFromBool(p.BucketSecurityValid, p.BucketSecurityReason, p.BucketSecurityMessage)),
		backupCurrent:          ifReachable(p.Reachable, computeBackupCurrent(br, now.Time)),
		metadataValid:          metadataValid,
		e2JobActive:            e2.jobActive,
		sampledIntegrityValid:  sampledIntegrityValid,
		e3JobActive:            e3.jobActive,
		fullRetrievalCompleted: fullRetrievalCompleted,
		e4JobActive:            e4.jobActive,
	})

	return r.Status().Patch(ctx, br, patch)
}

// conditionInputs lets applyConditions decide which optional E1 sub-
// conditions to emit (SizeWithinBudget only if budget set; etc.) and which
// to leave absent.
type conditionInputs struct {
	reachable           *metav1.Condition
	bucketSecurityValid *metav1.Condition
	backupCurrent       *metav1.Condition
	// metadataValid is the result of the most recent completed E2 Job.
	// nil means no completed E2 has been ingested yet (first cycle, or
	// the controller just started before any Job finished).
	metadataValid *metav1.Condition
	// e2JobActive is true if an E2 Job is currently running. Used to
	// pick the right Unknown reason for MetadataValid in the no-result
	// case (job is in flight vs. genuinely not yet available).
	e2JobActive bool
	// sampledIntegrityValid: same shape as metadataValid but for E3.
	// nil means no completed E3 has been ingested yet — combined with
	// "is E3 enabled at all" decides whether to emit the condition.
	sampledIntegrityValid *metav1.Condition
	e3JobActive           bool
	// fullRetrievalCompleted: same shape, for E4.
	fullRetrievalCompleted *metav1.Condition
	e4JobActive            bool
}

// applyConditions writes the full condition set onto br.Status.Conditions:
//   - E1 sub-conditions from the inputs (Reachable, BucketSecurityValid,
//     BackupCurrent) plus optional SizeWithinBudget (Unknown until
//     inventory ships)
//   - E2-E4 sub-conditions: present if their interval is set in the spec;
//     status Unknown until the corresponding pass ships
//   - Rollups: ProbeHealthy (E1), ValidationHealthy (E2-E4), Healthy
func (r *BackupRepositoryReconciler) applyConditions(
	br *aretev1alpha1.BackupRepository, in conditionInputs,
) {
	cs := &br.Status.Conditions

	// E1 sub-conditions
	setCondition(cs, aretev1alpha1.ConditionReachable, in.reachable)
	if in.bucketSecurityValid != nil {
		setCondition(cs, aretev1alpha1.ConditionBucketSecurityValid, in.bucketSecurityValid)
	} else {
		setCondition(cs, aretev1alpha1.ConditionBucketSecurityValid,
			condUnknown(aretev1alpha1.ReasonProbeSucceeded, "not yet evaluated this cycle"))
	}
	if in.backupCurrent != nil {
		setCondition(cs, aretev1alpha1.ConditionBackupCurrent, in.backupCurrent)
	} else {
		setCondition(cs, aretev1alpha1.ConditionBackupCurrent,
			condUnknown(aretev1alpha1.ReasonProbeSucceeded, "not yet evaluated this cycle"))
	}

	// SizeWithinBudget — only if budget is set. Compares
	// observedInventory.totalBytes against spec.expectedSizeBudget.
	if br.Spec.ExpectedSizeBudget != nil {
		setCondition(cs, aretev1alpha1.ConditionSizeWithinBudget,
			computeSizeWithinBudget(br))
	} else {
		removeCondition(cs, aretev1alpha1.ConditionSizeWithinBudget)
	}

	// MetadataValid (E2): preserve the latest result if we have one;
	// otherwise emit a clear Unknown distinguishing "Job in flight" from
	// "haven't run one yet."
	switch {
	case in.metadataValid != nil:
		setCondition(cs, aretev1alpha1.ConditionMetadataValid, in.metadataValid)
	case in.e2JobActive:
		setCondition(cs, aretev1alpha1.ConditionMetadataValid, condUnknown(
			aretev1alpha1.ReasonLayerTwoNotYetAvailable,
			"E2 validation Job in flight; result will land on next reconcile"))
	default:
		setCondition(cs, aretev1alpha1.ConditionMetadataValid, condUnknown(
			aretev1alpha1.ReasonLayerTwoNotYetAvailable,
			"no E2 validation has completed yet"))
	}

	// SampledIntegrityValid (E3): present only if E3 enabled. Preserve
	// latest result if available; otherwise distinguish "Job in flight"
	// from "no E3 has run yet."
	if br.Spec.SampledRetrievalInterval != nil {
		switch {
		case in.sampledIntegrityValid != nil:
			setCondition(cs, aretev1alpha1.ConditionSampledIntegrityValid, in.sampledIntegrityValid)
		case in.e3JobActive:
			setCondition(cs, aretev1alpha1.ConditionSampledIntegrityValid, condUnknown(
				aretev1alpha1.ReasonLayerTwoNotYetAvailable,
				"E3 sampled retrieval Job in flight; result will land on next reconcile"))
		default:
			setCondition(cs, aretev1alpha1.ConditionSampledIntegrityValid, condUnknown(
				aretev1alpha1.ReasonLayerTwoNotYetAvailable,
				"no E3 sampled retrieval has completed yet"))
		}
	} else {
		removeCondition(cs, aretev1alpha1.ConditionSampledIntegrityValid)
	}
	// FullRetrievalCompleted (E4): emit whenever E4 is meaningful for
	// this BR — i.e., scheduled (interval set), in-flight, or there's
	// a prior result to report. The annotation-only path (no interval)
	// must still surface a condition once E4 has run at least once;
	// otherwise the operator forces an E4, gets a status.lastFullRetrieval
	// payload, but sees no condition in `kubectl get br` (silent success).
	e4Meaningful := br.Spec.FullRetrievalInterval != nil ||
		br.Status.LastFullRetrieval != nil ||
		in.e4JobActive
	if e4Meaningful {
		switch {
		case in.fullRetrievalCompleted != nil:
			setCondition(cs, aretev1alpha1.ConditionFullRetrievalCompleted, in.fullRetrievalCompleted)
		case in.e4JobActive:
			setCondition(cs, aretev1alpha1.ConditionFullRetrievalCompleted, condUnknown(
				aretev1alpha1.ReasonLayerTwoNotYetAvailable,
				"E4 full retrieval Job in flight; result will land on next reconcile"))
		default:
			// Have a prior LastFullRetrieval but no new completedResult this
			// cycle — preserve the existing condition rather than blanking.
			if existing := apimeta.FindStatusCondition(br.Status.Conditions,
				aretev1alpha1.ConditionFullRetrievalCompleted); existing != nil {
				setCondition(cs, aretev1alpha1.ConditionFullRetrievalCompleted, existing)
			} else {
				setCondition(cs, aretev1alpha1.ConditionFullRetrievalCompleted, condUnknown(
					aretev1alpha1.ReasonLayerTwoNotYetAvailable,
					"no E4 full retrieval has completed yet"))
			}
		}
	} else {
		removeCondition(cs, aretev1alpha1.ConditionFullRetrievalCompleted)
	}

	// Rollups — recompute from the now-current condition set
	probeHealthy := rollupAND(*cs,
		aretev1alpha1.ConditionReachable,
		aretev1alpha1.ConditionBucketSecurityValid,
		aretev1alpha1.ConditionBackupCurrent,
		aretev1alpha1.ConditionSizeWithinBudget, // skipped if absent
	)
	setCondition(cs, aretev1alpha1.ConditionProbeHealthy, &probeHealthy)

	validationHealthy := rollupAND(*cs,
		aretev1alpha1.ConditionMetadataValid,
		aretev1alpha1.ConditionSampledIntegrityValid, // skipped if absent
		aretev1alpha1.ConditionFullRetrievalCompleted,
	)
	setCondition(cs, aretev1alpha1.ConditionValidationHealthy, &validationHealthy)

	overall := rollupAND(*cs,
		aretev1alpha1.ConditionProbeHealthy,
		aretev1alpha1.ConditionValidationHealthy,
	)
	setCondition(cs, aretev1alpha1.ConditionHealthy, &overall)
}

// computeSizeWithinBudget returns SizeWithinBudget True/False/Unknown:
//   - Unknown if observedInventory hasn't been recorded yet.
//   - True  if inventory.totalBytes <= spec.expectedSizeBudget.
//   - False (SizeBudgetExceeded) when over.
func computeSizeWithinBudget(br *aretev1alpha1.BackupRepository) *metav1.Condition {
	if br.Status.ObservedInventory == nil {
		return condUnknown(
			aretev1alpha1.ReasonLayerTwoNotYetAvailable,
			"observedInventory not yet recorded; first inventory probe pending",
		)
	}
	have := br.Status.ObservedInventory.TotalBytes
	budget := *br.Spec.ExpectedSizeBudget
	if have.Cmp(budget) <= 0 {
		return condTrue(
			aretev1alpha1.ReasonProbeSucceeded,
			fmt.Sprintf("inventory %s within budget %s", have.String(), budget.String()),
		)
	}
	return condFalse(
		aretev1alpha1.ReasonSizeBudgetExceeded,
		fmt.Sprintf("inventory %s exceeds budget %s", have.String(), budget.String()),
	)
}

// computeBackupCurrent applies the lag check: True iff
// claimedLastSuccessfulBackup is within spec.maxBackupLag of now.
// False with RepositoryEmpty if no backup has been detected at all.
func computeBackupCurrent(br *aretev1alpha1.BackupRepository, now time.Time) *metav1.Condition {
	last := br.Status.ClaimedLastSuccessfulBackup
	if last == nil {
		return condFalse(
			aretev1alpha1.ReasonRepositoryEmpty,
			"no backup detected in repository",
		)
	}
	lag := now.Sub(last.Time)
	if lag <= br.Spec.MaxBackupLag.Duration {
		return condTrue(
			aretev1alpha1.ReasonProbeSucceeded,
			fmt.Sprintf("most recent backup is %s old (within %s)",
				lag.Round(time.Second), br.Spec.MaxBackupLag.Duration),
		)
	}
	return condFalse(
		aretev1alpha1.ReasonBackupLagExceeded,
		fmt.Sprintf("most recent backup is %s old (exceeds %s)",
			lag.Round(time.Second), br.Spec.MaxBackupLag.Duration),
	)
}

// ifReachable returns the given condition if reachable; otherwise an
// Unknown condition with a "not evaluated this cycle" message — used to
// gate downstream sub-conditions on phase-1 success without leaving them
// stale-True from a previous reconcile.
func ifReachable(reachable bool, c *metav1.Condition) *metav1.Condition {
	if !reachable {
		return condUnknown(aretev1alpha1.ReasonS3Unreachable, "skipped: phase-1 reachability failed")
	}
	return c
}

// ----- condition helpers -----

func condTrue(reason, message string) *metav1.Condition {
	return &metav1.Condition{Status: metav1.ConditionTrue, Reason: reason, Message: message}
}
func condFalse(reason, message string) *metav1.Condition {
	return &metav1.Condition{Status: metav1.ConditionFalse, Reason: reason, Message: message}
}
func condUnknown(reason, message string) *metav1.Condition {
	return &metav1.Condition{Status: metav1.ConditionUnknown, Reason: reason, Message: message}
}

func condFromBool(ok bool, reason, message string) *metav1.Condition {
	if ok {
		return condTrue(reason, message)
	}
	return condFalse(reason, message)
}

// setCondition writes the given condition, copying status/reason/message
// onto a properly-typed metav1.Condition with the supplied Type.
func setCondition(cs *[]metav1.Condition, condType string, c *metav1.Condition) {
	if c == nil {
		return
	}
	out := *c
	out.Type = condType
	apimeta.SetStatusCondition(cs, out)
}

func removeCondition(cs *[]metav1.Condition, condType string) {
	apimeta.RemoveStatusCondition(cs, condType)
}

// rollupAND computes the AND of the given condition types, skipping any
// that are absent from the set. Result:
//   - False if any present condition is False
//   - Unknown if any present condition is Unknown (and none is False)
//   - True if all present conditions are True
//   - Unknown if no condition of any of the requested types is present
func rollupAND(cs []metav1.Condition, condTypes ...string) metav1.Condition {
	var anyFalse, anyUnknown, anyTrue bool
	var falseMsg, unknownMsg string
	for _, t := range condTypes {
		c := apimeta.FindStatusCondition(cs, t)
		if c == nil {
			continue
		}
		switch c.Status {
		case metav1.ConditionFalse:
			anyFalse = true
			if falseMsg == "" {
				falseMsg = fmt.Sprintf("%s: %s", t, c.Message)
			}
		case metav1.ConditionUnknown:
			anyUnknown = true
			if unknownMsg == "" {
				unknownMsg = fmt.Sprintf("%s: %s", t, c.Message)
			}
		case metav1.ConditionTrue:
			anyTrue = true
		}
	}
	switch {
	case anyFalse:
		return metav1.Condition{
			Status:  metav1.ConditionFalse,
			Reason:  "RolledUp",
			Message: falseMsg,
		}
	case anyUnknown:
		return metav1.Condition{
			Status:  metav1.ConditionUnknown,
			Reason:  aretev1alpha1.ReasonLayerTwoNotYetAvailable,
			Message: unknownMsg,
		}
	case anyTrue:
		return metav1.Condition{
			Status:  metav1.ConditionTrue,
			Reason:  aretev1alpha1.ReasonProbeSucceeded,
			Message: "all required sub-conditions True",
		}
	default:
		return metav1.Condition{
			Status:  metav1.ConditionUnknown,
			Reason:  aretev1alpha1.ReasonLayerTwoNotYetAvailable,
			Message: "no sub-conditions present",
		}
	}
}

// ----- secret watch -----

// mapSecretToRepositories returns reconcile requests for every
// BackupRepository that references the given Secret as its credentialsSecret.
// Wired via Watches() so credential rotation triggers an immediate reprobe
// rather than waiting for the next probeInterval tick.
func (r *BackupRepositoryReconciler) mapSecretToRepositories(
	ctx context.Context, obj client.Object,
) []reconcile.Request {
	secret, ok := obj.(*corev1.Secret)
	if !ok {
		return nil
	}
	var list aretev1alpha1.BackupRepositoryList
	if err := r.List(ctx, &list); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range list.Items {
		ref := list.Items[i].Spec.S3.CredentialsSecret
		if ref.Name == secret.Name && ref.Namespace == secret.Namespace {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(&list.Items[i]),
			})
		}
	}
	return requests
}

// SetupWithManager sets up the controller with the Manager.
//
// Reconcile triggers:
//   - For()    — spec changes (GenerationChangedPredicate) OR
//     `arete.io/force-revalidate` annotation changes (custom
//     predicate). Status-only updates are filtered to avoid the
//     self-feeding loop that LastProbedAt-on-every-cycle would
//     otherwise produce.
//   - Owns()   — Job state changes (created/scheduled/complete/deleted).
//   - Watches()— Secret create/update/delete via mapSecretToRepositories.
//   - Periodic — Result{RequeueAfter: probeInterval} from each Reconcile.
func (r *BackupRepositoryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aretev1alpha1.BackupRepository{},
			builder.WithPredicates(predicate.Or(
				predicate.GenerationChangedPredicate{},
				forceRevalidatePredicate{},
			))).
		Owns(&batchv1.Job{}).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.mapSecretToRepositories),
		).
		Named("backuprepository").
		Complete(r)
}

// forceRevalidatePredicate fires Reconcile when the
// `arete.io/force-revalidate` annotation changes value. Combined with
// GenerationChangedPredicate via predicate.Or so we still ignore other
// metadata changes (labels, owner references, status sub-resource).
//
// Without this, an operator running
//
//	kubectl annotate br foo arete.io/force-revalidate=$(date -u +%FT%TZ)
//
// would not wake the reconciler until the next probeInterval (default
// 10 min) since annotations don't bump generation.
type forceRevalidatePredicate struct {
	predicate.Funcs
}

func (forceRevalidatePredicate) Create(e event.CreateEvent) bool {
	// Don't trigger on create — the For-watch's GenerationChangedPredicate
	// already covers the create case via its own Create handler.
	return false
}

func (forceRevalidatePredicate) Update(e event.UpdateEvent) bool {
	if e.ObjectOld == nil || e.ObjectNew == nil {
		return false
	}
	oldRev := e.ObjectOld.GetAnnotations()[aretev1alpha1.AnnotationForceRevalidate]
	newRev := e.ObjectNew.GetAnnotations()[aretev1alpha1.AnnotationForceRevalidate]
	if oldRev != newRev {
		return true
	}
	oldE4 := e.ObjectOld.GetAnnotations()[aretev1alpha1.AnnotationForceE4]
	newE4 := e.ObjectNew.GetAnnotations()[aretev1alpha1.AnnotationForceE4]
	return oldE4 != newE4
}

func (forceRevalidatePredicate) Delete(e event.DeleteEvent) bool {
	return false
}

func (forceRevalidatePredicate) Generic(e event.GenericEvent) bool {
	return false
}
