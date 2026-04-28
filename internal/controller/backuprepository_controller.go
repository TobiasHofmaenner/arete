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
		return r.recordProbeFailure(ctx, &br, "CredentialsUnavailable", err.Error())
	}

	result := probeRepository(ctx, br.Spec, creds)

	if err := r.recordProbeResult(ctx, &br, result); err != nil {
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

// recordProbeFailure writes a Reachable=False condition with the given
// reason+message and requeues at the spec's probe interval. Used for
// pre-probe failures (e.g. credentials missing) where we never reached S3.
func (r *BackupRepositoryReconciler) recordProbeFailure(
	ctx context.Context, br *aretev1alpha1.BackupRepository, reason, message string,
) (ctrl.Result, error) {
	patch := client.MergeFrom(br.DeepCopy())
	now := metav1.Now()
	br.Status.LastProbedAt = &now
	br.Status.ObservedGeneration = br.Generation
	apimeta.SetStatusCondition(&br.Status.Conditions, metav1.Condition{
		Type:    aretev1alpha1.ConditionReachable,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: message,
	})
	if err := r.Status().Patch(ctx, br, patch); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: br.Spec.ProbeInterval.Duration}, nil
}

// recordProbeResult applies a successful probe's outcome to status.
func (r *BackupRepositoryReconciler) recordProbeResult(
	ctx context.Context, br *aretev1alpha1.BackupRepository, p probeResult,
) error {
	patch := client.MergeFrom(br.DeepCopy())
	now := metav1.Now()
	br.Status.LastProbedAt = &now
	br.Status.ObservedGeneration = br.Generation
	br.Status.DetectedFormat = p.DetectedFormat
	br.Status.DetectedVersion = p.DetectedVersion

	condStatus := metav1.ConditionTrue
	if !p.Reachable {
		condStatus = metav1.ConditionFalse
	}
	apimeta.SetStatusCondition(&br.Status.Conditions, metav1.Condition{
		Type:    aretev1alpha1.ConditionReachable,
		Status:  condStatus,
		Reason:  p.Reason,
		Message: p.Message,
	})
	return r.Status().Patch(ctx, br, patch)
}

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
