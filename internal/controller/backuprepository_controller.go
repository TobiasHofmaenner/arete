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
	"fmt"
	"strings"
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
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aretev1alpha1 "github.com/TobiasHofmaenner/arete/api/v1alpha1"
)

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
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	creds, err := r.resolveCredentials(ctx, &br)
	if err != nil {
		log.Info("credentials unavailable", "err", err)
		return r.recordCredentialsFailure(ctx, &br, err)
	}

	// E1 probe (in-process; cheap)
	probe := probeRepository(ctx, br.Spec, creds)

	// E2 Job lifecycle: process any completed Job, maybe spawn a new one
	e2 := r.processE2(ctx, &br)

	// E3 Job lifecycle: opt-in via spec.sampledRetrievalInterval
	e3 := r.processE3(ctx, &br, creds)

	// E4 Job lifecycle: opt-in via spec.fullRetrievalInterval
	e4 := r.processE4(ctx, &br)

	if err := r.applyStatus(ctx, &br, probe, e2, e3, e4); err != nil {
		log.Error(err, "failed to update status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: br.Spec.ProbeInterval.Duration}, nil
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
// any newly-completed Job, and spawns a fresh Job when due.
func (r *BackupRepositoryReconciler) processE2(
	ctx context.Context, br *aretev1alpha1.BackupRepository,
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
		}
	}

	// Spawn a new Job if due and none currently running.
	if !out.jobActive && shouldSpawnE2(br, time.Now()) {
		if _, err := r.spawnE2Job(ctx, br); err != nil {
			log.Error(err, "spawn e2 job")
		} else {
			out.jobActive = true
			log.Info("spawned E2 job", "format", br.Spec.Format)
		}
	}

	return out
}

// processE3 mirrors processE2 for sampled retrieval. Skipped entirely
// when the level is not enabled (sampledRetrievalInterval nil) — the
// SampledIntegrityValid condition stays absent or stale-True from the
// previous run.
func (r *BackupRepositoryReconciler) processE3(
	ctx context.Context, br *aretev1alpha1.BackupRepository, creds S3Credentials,
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
		}
	}

	if !out.jobActive && shouldSpawnE3(br, br.Status.VerifiedLastSampledRetrievalAt, time.Now()) {
		if _, err := r.spawnE3Job(ctx, br, creds); err != nil {
			log.Error(err, "spawn e3 job")
		} else {
			out.jobActive = true
			log.Info("spawned E3 job", "format", br.Spec.Format)
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
	ctx context.Context, br *aretev1alpha1.BackupRepository,
) e4Outcome {
	log := logf.FromContext(ctx)
	out := e4Outcome{}
	if br.Spec.FullRetrievalInterval == nil {
		return out
	}

	jobs, err := r.listE4Jobs(ctx, br)
	if err != nil {
		log.Error(err, "list e4 jobs")
		return out
	}

	if active := firstActiveJob(jobs); active != nil {
		out.jobActive = true
	}

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
		}
	}

	var lastE4 *metav1.Time
	if br.Status.LastFullRetrieval != nil {
		lastE4 = &br.Status.LastFullRetrieval.CompletedAt
	}
	if !out.jobActive && shouldSpawnE4(br, lastE4, time.Now()) {
		if _, err := r.spawnE4Job(ctx, br); err != nil {
			log.Error(err, "spawn e4 job")
		} else {
			out.jobActive = true
			log.Info("spawned E4 job", "format", br.Spec.Format)
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
			return S3Credentials{}, fmt.Errorf("secret %s not found", key)
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

// recordCredentialsFailure handles the pre-probe failure case (no creds, so
// we can't reach S3 at all). Sets every E1 sub-condition to False/Unknown
// with a clear reason, blanks claimed*/observed*/verified* data, and
// requeues at probeInterval.
func (r *BackupRepositoryReconciler) recordCredentialsFailure(
	ctx context.Context, br *aretev1alpha1.BackupRepository, credErr error,
) (ctrl.Result, error) {
	patch := client.MergeFrom(br.DeepCopy())
	now := metav1.Now()
	br.Status.LastProbedAt = &now
	br.Status.ObservedGeneration = br.Generation

	r.applyConditions(br, conditionInputs{
		reachable: condFalse(aretev1alpha1.ReasonCredentialsUnavailable, credErr.Error()),
	})

	if err := r.Status().Patch(ctx, br, patch); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: br.Spec.ProbeInterval.Duration}, nil
}

// applyStatus computes every condition and updates the structured status
// fields from the probe + e2 + e3 + e4 outcomes, then patches the
// BackupRepository.
func (r *BackupRepositoryReconciler) applyStatus(
	ctx context.Context, br *aretev1alpha1.BackupRepository,
	p probeResult, e2 e2Outcome, e3 e3Outcome, e4 e4Outcome,
) error {
	patch := client.MergeFrom(br.DeepCopy())
	now := metav1.Now()

	br.Status.LastProbedAt = &now
	br.Status.ObservedGeneration = br.Generation
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

	// SizeWithinBudget — only if budget is set; Unknown until inventory ships
	if br.Spec.ExpectedSizeBudget != nil {
		setCondition(cs, aretev1alpha1.ConditionSizeWithinBudget, condUnknown(
			aretev1alpha1.ReasonLayerTwoNotYetAvailable,
			"observedInventory not yet implemented (Pass 3-inventory)"))
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
	// FullRetrievalCompleted (E4): present only if E4 enabled. Same
	// state machine as E3: latest result wins; otherwise distinguish
	// in-flight from never-run.
	if br.Spec.FullRetrievalInterval != nil {
		switch {
		case in.fullRetrievalCompleted != nil:
			setCondition(cs, aretev1alpha1.ConditionFullRetrievalCompleted, in.fullRetrievalCompleted)
		case in.e4JobActive:
			setCondition(cs, aretev1alpha1.ConditionFullRetrievalCompleted, condUnknown(
				aretev1alpha1.ReasonLayerTwoNotYetAvailable,
				"E4 full retrieval Job in flight; result will land on next reconcile"))
		default:
			setCondition(cs, aretev1alpha1.ConditionFullRetrievalCompleted, condUnknown(
				aretev1alpha1.ReasonLayerTwoNotYetAvailable,
				"no E4 full retrieval has completed yet"))
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
//   - For()    — spec changes only (GenerationChangedPredicate). Without
//     this filter, every status patch we make would re-trigger
//     Reconcile, producing a tight self-feeding loop because
//     LastProbedAt updates on every cycle.
//   - Owns()   — Job state changes (created/scheduled/complete/deleted).
//   - Watches()— Secret create/update/delete via mapSecretToRepositories.
//   - Periodic — Result{RequeueAfter: probeInterval} from each Reconcile.
func (r *BackupRepositoryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aretev1alpha1.BackupRepository{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&batchv1.Job{}).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.mapSecretToRepositories),
		).
		Named("backuprepository").
		Complete(r)
}
