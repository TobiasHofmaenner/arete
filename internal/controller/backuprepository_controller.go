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
	"time"

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
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aretev1alpha1 "github.com/TobiasHofmaenner/arete/api/v1alpha1"
)

// BackupRepositoryReconciler reconciles a BackupRepository object
type BackupRepositoryReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=arete.arete.io,resources=backuprepositories,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=arete.arete.io,resources=backuprepositories/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=arete.arete.io,resources=backuprepositories/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

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

	result := probeRepository(ctx, br.Spec, creds)

	if err := r.applyStatus(ctx, &br, result); err != nil {
		log.Error(err, "failed to update status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: br.Spec.ProbeInterval.Duration}, nil
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

	access := string(secret.Data["AWS_ACCESS_KEY_ID"])
	secretKey := string(secret.Data["AWS_SECRET_ACCESS_KEY"])
	if access == "" || secretKey == "" {
		return S3Credentials{}, fmt.Errorf(
			"secret %s missing required keys AWS_ACCESS_KEY_ID and/or AWS_SECRET_ACCESS_KEY",
			key,
		)
	}
	return S3Credentials{
		AccessKeyID:     access,
		SecretAccessKey: secretKey,
		SessionToken:    string(secret.Data["AWS_SESSION_TOKEN"]),
	}, nil
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
// fields (claimedLatestBackup, claimedLastSuccessfulBackup, detectedFormat/
// Version) from the probe result, then patches the BackupRepository.
func (r *BackupRepositoryReconciler) applyStatus(
	ctx context.Context, br *aretev1alpha1.BackupRepository, p probeResult,
) error {
	patch := client.MergeFrom(br.DeepCopy())
	now := metav1.Now()

	br.Status.LastProbedAt = &now
	br.Status.ObservedGeneration = br.Generation
	br.Status.DetectedFormat = p.DetectedFormat
	br.Status.DetectedVersion = p.DetectedVersion
	br.Status.ClaimedLatestBackup = p.LatestBackup
	if p.LatestBackup != nil {
		ts := p.LatestBackup.CreatedAt
		br.Status.ClaimedLastSuccessfulBackup = &ts
	} else {
		br.Status.ClaimedLastSuccessfulBackup = nil
	}

	r.applyConditions(br, conditionInputs{
		reachable: condFromBool(p.Reachable, p.ReachableReason, p.ReachableMessage),
		bucketSecurityValid: ifReachable(p.Reachable,
			condFromBool(p.BucketSecurityValid, p.BucketSecurityReason, p.BucketSecurityMessage)),
		backupCurrent: ifReachable(p.Reachable, computeBackupCurrent(br, now.Time)),
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

	// E2-E4 sub-conditions — Unknown until the corresponding pass lands
	setCondition(cs, aretev1alpha1.ConditionMetadataValid, condUnknown(
		aretev1alpha1.ReasonLayerTwoNotYetAvailable,
		"E2 metadata validation Job not yet implemented (Pass 3b)"))

	if br.Spec.SampledRetrievalInterval != nil {
		setCondition(cs, aretev1alpha1.ConditionSampledIntegrityValid, condUnknown(
			aretev1alpha1.ReasonLayerTwoNotYetAvailable,
			"E3 sampled retrieval Job not yet implemented (Pass 3c)"))
	} else {
		removeCondition(cs, aretev1alpha1.ConditionSampledIntegrityValid)
	}
	if br.Spec.FullRetrievalInterval != nil {
		setCondition(cs, aretev1alpha1.ConditionFullRetrievalCompleted, condUnknown(
			aretev1alpha1.ReasonLayerTwoNotYetAvailable,
			"E4 full retrieval Job not yet implemented (Pass 3c)"))
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
func rollupAND(cs []metav1.Condition, types ...string) metav1.Condition {
	var anyFalse, anyUnknown, anyTrue bool
	var falseMsg, unknownMsg string
	for _, t := range types {
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
func (r *BackupRepositoryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aretev1alpha1.BackupRepository{}).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.mapSecretToRepositories),
			builder.WithPredicates(),
		).
		Named("backuprepository").
		Complete(r)
}
