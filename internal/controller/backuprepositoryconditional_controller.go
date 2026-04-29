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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aretev1alpha1 "github.com/TobiasHofmaenner/arete/api/v1alpha1"
)

const (
	// fieldManager is the SSA field manager identifier the controller
	// uses when applying child resources. Stable across versions.
	brConditionalFieldManager = "arete-backup-repository-conditional"
)

// BackupRepositoryConditionalReconciler reconciles BackupRepositoryConditional.
type BackupRepositoryConditionalReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=arete.arete.io,resources=backuprepositoryconditionals,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=arete.arete.io,resources=backuprepositoryconditionals/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=arete.arete.io,resources=backuprepositoryconditionals/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// Note on child-resource RBAC: the controller materializes arbitrary
// K8s objects and therefore needs broad write permissions on whatever
// GVKs users plug in. The chart grants a curated default (cnpg
// Cluster) and documents how to extend; users add their own
// ClusterRole bindings for additional GVKs.

func (r *BackupRepositoryConditionalReconciler) Reconcile(
	ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var brc aretev1alpha1.BackupRepositoryConditional
	if err := r.Get(ctx, req.NamespacedName, &brc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	patch := client.MergeFrom(brc.DeepCopy())
	brc.Status.ObservedGeneration = brc.Generation

	// 1. Resolve BackupRepository (cluster-scoped).
	var br aretev1alpha1.BackupRepository
	brKey := types.NamespacedName{Name: brc.Spec.RepositoryRef.Name}
	if err := r.Get(ctx, brKey, &br); err != nil {
		if apierrors.IsNotFound(err) {
			r.setBRConditionalCondition(&brc,
				aretev1alpha1.BRConditionalBackupRepositoryFound, metav1.ConditionFalse,
				aretev1alpha1.BRConditionalReasonRepositoryNotFound,
				fmt.Sprintf("BackupRepository %q not found", brKey.Name))
			r.setBRConditionalCondition(&brc,
				aretev1alpha1.BRConditionalDecided, metav1.ConditionFalse,
				aretev1alpha1.BRConditionalReasonRepositoryNotFound,
				"cannot decide without a referenced BackupRepository")
			return ctrl.Result{}, r.Status().Patch(ctx, &brc, patch)
		}
		return ctrl.Result{}, err
	}
	r.setBRConditionalCondition(&brc,
		aretev1alpha1.BRConditionalBackupRepositoryFound, metav1.ConditionTrue,
		aretev1alpha1.BRConditionalReasonOK, fmt.Sprintf("resolved %q", br.Name))

	// 2. Derive state and pick variant.
	state := deriveBRState(&br)
	brc.Status.ObservedRepositoryState = string(state)
	variant, slot := pickVariant(brc.Spec, state)
	if variant == nil {
		// Refusal is intentional: no variant defined for this state.
		// Don't touch any previously-applied child; just record why.
		r.setBRConditionalCondition(&brc,
			aretev1alpha1.BRConditionalDecided, metav1.ConditionFalse,
			aretev1alpha1.BRConditionalReasonNoVariantForState,
			fmt.Sprintf("no variant defined for state %q", state))
		return ctrl.Result{}, r.Status().Patch(ctx, &brc, patch)
	}

	// 3. Read manifest source.
	rawYAML, sourceCond, err := r.readManifestSource(ctx, brc.Namespace, variant.ManifestRef)
	r.setBRConditionalCondition(&brc, sourceCond.Type, sourceCond.Status, sourceCond.Reason, sourceCond.Message)
	if err != nil {
		r.setBRConditionalCondition(&brc,
			aretev1alpha1.BRConditionalDecided, metav1.ConditionFalse,
			sourceCond.Reason, sourceCond.Message)
		return ctrl.Result{}, r.Status().Patch(ctx, &brc, patch)
	}

	// 4. Parse YAML into an unstructured object.
	var obj unstructured.Unstructured
	if err := yaml.Unmarshal(rawYAML, &obj.Object); err != nil {
		r.setBRConditionalCondition(&brc,
			aretev1alpha1.BRConditionalManifestParsed, metav1.ConditionFalse,
			aretev1alpha1.BRConditionalReasonManifestParseFailed, err.Error())
		r.setBRConditionalCondition(&brc,
			aretev1alpha1.BRConditionalDecided, metav1.ConditionFalse,
			aretev1alpha1.BRConditionalReasonManifestParseFailed, err.Error())
		return ctrl.Result{}, r.Status().Patch(ctx, &brc, patch)
	}
	if obj.GetKind() == "" || obj.GetAPIVersion() == "" || obj.GetName() == "" {
		msg := "manifest missing apiVersion/kind/metadata.name"
		r.setBRConditionalCondition(&brc,
			aretev1alpha1.BRConditionalManifestParsed, metav1.ConditionFalse,
			aretev1alpha1.BRConditionalReasonManifestParseFailed, msg)
		r.setBRConditionalCondition(&brc,
			aretev1alpha1.BRConditionalDecided, metav1.ConditionFalse,
			aretev1alpha1.BRConditionalReasonManifestParseFailed, msg)
		return ctrl.Result{}, r.Status().Patch(ctx, &brc, patch)
	}
	r.setBRConditionalCondition(&brc,
		aretev1alpha1.BRConditionalManifestParsed, metav1.ConditionTrue,
		aretev1alpha1.BRConditionalReasonOK,
		fmt.Sprintf("%s/%s %q", obj.GetAPIVersion(), obj.GetKind(), obj.GetName()))

	// 5. Default the namespace to the BRC's namespace if the manifest
	//    didn't specify one. Cluster-scoped resources will ignore this.
	if obj.GetNamespace() == "" {
		obj.SetNamespace(brc.Namespace)
	}

	// 6. Stamp ownerReference. The child is GC'd when the BRC goes.
	if err := controllerutil.SetControllerReference(&brc, &obj, r.Scheme); err != nil {
		r.setBRConditionalCondition(&brc,
			aretev1alpha1.BRConditionalChildApplied, metav1.ConditionFalse,
			aretev1alpha1.BRConditionalReasonApplyFailed,
			fmt.Sprintf("set owner ref: %s", err.Error()))
		r.setBRConditionalCondition(&brc,
			aretev1alpha1.BRConditionalDecided, metav1.ConditionFalse,
			aretev1alpha1.BRConditionalReasonApplyFailed, err.Error())
		return ctrl.Result{}, r.Status().Patch(ctx, &brc, patch)
	}

	// 7. Server-side apply with a stable field manager. Force=true so
	//    re-reconciles after a manifest edit overwrite previous fields
	//    owned by us. The ApplyConfigurationFromUnstructured helper
	//    converts our parsed unstructured.Unstructured into the typed
	//    runtime.ApplyConfiguration the new client.Apply API expects.
	if err := r.Apply(ctx, client.ApplyConfigurationFromUnstructured(&obj),
		&client.ApplyOptions{
			FieldManager: brConditionalFieldManager,
			Force:        ptrBool(true),
		},
	); err != nil {
		// Special-case immutable conflicts: cnpg's bootstrap.* is
		// immutable after creation, so a state flip after first apply
		// will get rejected. That's the SAFETY behavior, not a bug.
		// Surface it clearly so users understand the previous decision
		// is sticky-by-target-enforcement.
		reason := aretev1alpha1.BRConditionalReasonApplyFailed
		if isImmutableFieldError(err) {
			reason = aretev1alpha1.BRConditionalReasonImmutableConflict
		}
		r.setBRConditionalCondition(&brc,
			aretev1alpha1.BRConditionalChildApplied, metav1.ConditionFalse,
			reason, err.Error())
		r.setBRConditionalCondition(&brc,
			aretev1alpha1.BRConditionalDecided, metav1.ConditionFalse,
			reason, err.Error())
		log.Error(err, "apply child", "gvk", obj.GroupVersionKind(), "name", obj.GetName())
		return ctrl.Result{}, r.Status().Patch(ctx, &brc, patch)
	}

	// 8. Success — record decision + applied-ref.
	now := metav1.Now()
	brc.Status.Decided = slot
	brc.Status.DecidedAt = &now
	brc.Status.AppliedRef = &aretev1alpha1.AppliedResourceRef{
		APIVersion: obj.GetAPIVersion(),
		Kind:       obj.GetKind(),
		Namespace:  obj.GetNamespace(),
		Name:       obj.GetName(),
	}
	r.setBRConditionalCondition(&brc,
		aretev1alpha1.BRConditionalChildApplied, metav1.ConditionTrue,
		aretev1alpha1.BRConditionalReasonOK,
		fmt.Sprintf("applied %s/%s %q", obj.GetAPIVersion(), obj.GetKind(), obj.GetName()))
	r.setBRConditionalCondition(&brc,
		aretev1alpha1.BRConditionalDecided, metav1.ConditionTrue,
		aretev1alpha1.BRConditionalReasonOK,
		fmt.Sprintf("variant %q applied for state %q", slot, state))

	return ctrl.Result{}, r.Status().Patch(ctx, &brc, patch)
}

// brState is the discrete state derived from a BackupRepository's
// observable signals. Drives variant selection.
type brState string

const (
	brStateHealthy  brState = "healthy"
	brStateEmpty    brState = "empty"
	brStateDegraded brState = "degraded"
)

// deriveBRState collapses the BR's status into one of three discrete
// states. Order matters: emptiness is checked BEFORE health rollup so a
// freshly-created repo with zero objects routes to whenEmpty rather
// than whenDegraded (which it would technically be — Healthy=Unknown
// because no E2 has run yet).
func deriveBRState(br *aretev1alpha1.BackupRepository) brState {
	// Empty: no successful backup AND no objects yet. Catches the
	// fresh-tenant scenario unambiguously.
	noBackup := br.Status.ClaimedLastSuccessfulBackup == nil
	noObjects := br.Status.ObservedInventory != nil && br.Status.ObservedInventory.ObjectCount == 0
	if noBackup && noObjects {
		return brStateEmpty
	}

	healthy := apimeta.FindStatusCondition(br.Status.Conditions, aretev1alpha1.ConditionHealthy)
	if healthy != nil && healthy.Status == metav1.ConditionTrue {
		return brStateHealthy
	}
	return brStateDegraded
}

// pickVariant returns the variant slot matching the state, plus the
// slot's name (for status reporting).
func pickVariant(spec aretev1alpha1.BackupRepositoryConditionalSpec, state brState) (*aretev1alpha1.VariantSpec, string) {
	switch state {
	case brStateHealthy:
		return spec.WhenHealthy, "whenHealthy"
	case brStateEmpty:
		return spec.WhenEmpty, "whenEmpty"
	case brStateDegraded:
		return spec.WhenDegraded, "whenDegraded"
	}
	return nil, ""
}

// readManifestSource fetches the manifest YAML from either the
// referenced ConfigMap or Secret in the BRC's namespace. Returns the
// raw bytes, a status condition describing the outcome, and an error
// if the fetch failed terminally.
func (r *BackupRepositoryConditionalReconciler) readManifestSource(
	ctx context.Context, namespace string, ref aretev1alpha1.ManifestRef,
) ([]byte, metav1.Condition, error) {
	switch {
	case ref.ConfigMap != nil && ref.Secret != nil:
		msg := "manifestRef must set exactly one of configMap or secret, not both"
		return nil, metav1.Condition{
			Type:    aretev1alpha1.BRConditionalManifestSourceFound,
			Status:  metav1.ConditionFalse,
			Reason:  aretev1alpha1.BRConditionalReasonManifestSourceMiss,
			Message: msg,
		}, fmt.Errorf("%s", msg)

	case ref.ConfigMap != nil:
		var cm corev1.ConfigMap
		key := types.NamespacedName{Namespace: namespace, Name: ref.ConfigMap.Name}
		if err := r.Get(ctx, key, &cm); err != nil {
			return nil, metav1.Condition{
				Type:    aretev1alpha1.BRConditionalManifestSourceFound,
				Status:  metav1.ConditionFalse,
				Reason:  aretev1alpha1.BRConditionalReasonManifestSourceMiss,
				Message: fmt.Sprintf("ConfigMap %s: %s", key, err.Error()),
			}, err
		}
		raw, ok := cm.Data[ref.ConfigMap.Key]
		if !ok {
			msg := fmt.Sprintf("ConfigMap %s has no key %q", key, ref.ConfigMap.Key)
			return nil, metav1.Condition{
				Type:    aretev1alpha1.BRConditionalManifestSourceFound,
				Status:  metav1.ConditionFalse,
				Reason:  aretev1alpha1.BRConditionalReasonManifestSourceMiss,
				Message: msg,
			}, fmt.Errorf("%s", msg)
		}
		return []byte(raw), metav1.Condition{
			Type:    aretev1alpha1.BRConditionalManifestSourceFound,
			Status:  metav1.ConditionTrue,
			Reason:  aretev1alpha1.BRConditionalReasonOK,
			Message: fmt.Sprintf("ConfigMap %s key %q (%d bytes)", key, ref.ConfigMap.Key, len(raw)),
		}, nil

	case ref.Secret != nil:
		var secret corev1.Secret
		key := types.NamespacedName{Namespace: namespace, Name: ref.Secret.Name}
		if err := r.Get(ctx, key, &secret); err != nil {
			return nil, metav1.Condition{
				Type:    aretev1alpha1.BRConditionalManifestSourceFound,
				Status:  metav1.ConditionFalse,
				Reason:  aretev1alpha1.BRConditionalReasonManifestSourceMiss,
				Message: fmt.Sprintf("Secret %s: %s", key, err.Error()),
			}, err
		}
		raw, ok := secret.Data[ref.Secret.Key]
		if !ok {
			msg := fmt.Sprintf("Secret %s has no key %q", key, ref.Secret.Key)
			return nil, metav1.Condition{
				Type:    aretev1alpha1.BRConditionalManifestSourceFound,
				Status:  metav1.ConditionFalse,
				Reason:  aretev1alpha1.BRConditionalReasonManifestSourceMiss,
				Message: msg,
			}, fmt.Errorf("%s", msg)
		}
		return raw, metav1.Condition{
			Type:    aretev1alpha1.BRConditionalManifestSourceFound,
			Status:  metav1.ConditionTrue,
			Reason:  aretev1alpha1.BRConditionalReasonOK,
			Message: fmt.Sprintf("Secret %s key %q (%d bytes)", key, ref.Secret.Key, len(raw)),
		}, nil
	}

	msg := "manifestRef must set one of configMap or secret"
	return nil, metav1.Condition{
		Type:    aretev1alpha1.BRConditionalManifestSourceFound,
		Status:  metav1.ConditionFalse,
		Reason:  aretev1alpha1.BRConditionalReasonManifestSourceMiss,
		Message: msg,
	}, fmt.Errorf("%s", msg)
}

// isImmutableFieldError detects K8s server rejections of changes to
// immutable fields. Used to surface the cnpg-bootstrap-is-immutable
// behavior cleanly rather than as a generic apply failure.
func isImmutableFieldError(err error) bool {
	if err == nil {
		return false
	}
	// API server returns 422 Invalid with "field is immutable" in the
	// status message. Cheap substring check is robust enough.
	return strings.Contains(strings.ToLower(err.Error()), "immutable")
}

// setBRConditionalCondition sets a condition on the BRC's status,
// preserving lastTransitionTime semantics via apimeta.SetStatusCondition.
func (r *BackupRepositoryConditionalReconciler) setBRConditionalCondition(
	brc *aretev1alpha1.BackupRepositoryConditional,
	condType string, status metav1.ConditionStatus, reason, message string,
) {
	apimeta.SetStatusCondition(&brc.Status.Conditions, metav1.Condition{
		Type:    condType,
		Status:  status,
		Reason:  reason,
		Message: message,
	})
}

// mapBRToConditionals returns reconcile requests for every
// BackupRepositoryConditional that references the given BackupRepository.
// Wired via Watches() so a BR state change immediately re-evaluates
// dependent BRCs without waiting for periodic resync.
func (r *BackupRepositoryConditionalReconciler) mapBRToConditionals(
	ctx context.Context, obj client.Object,
) []reconcile.Request {
	br, ok := obj.(*aretev1alpha1.BackupRepository)
	if !ok {
		return nil
	}
	var list aretev1alpha1.BackupRepositoryConditionalList
	if err := r.List(ctx, &list); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range list.Items {
		if list.Items[i].Spec.RepositoryRef.Name == br.Name {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(&list.Items[i]),
			})
		}
	}
	return requests
}

// mapManifestSourceToConditionals returns reconcile requests for every
// BRC that references the given ConfigMap/Secret as its manifest source.
// Wired via Watches() so editing the source manifest re-applies the
// child without waiting for the next reconcile tick.
func (r *BackupRepositoryConditionalReconciler) mapManifestSourceToConditionals(
	ctx context.Context, obj client.Object,
) []reconcile.Request {
	name, namespace := obj.GetName(), obj.GetNamespace()
	_, isCM := obj.(*corev1.ConfigMap)
	_, isSecret := obj.(*corev1.Secret)
	if !isCM && !isSecret {
		return nil
	}
	var list aretev1alpha1.BackupRepositoryConditionalList
	if err := r.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range list.Items {
		brc := &list.Items[i]
		if matchesAnyVariantManifestRef(brc, name, isCM) {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(brc),
			})
		}
	}
	return requests
}

func matchesAnyVariantManifestRef(brc *aretev1alpha1.BackupRepositoryConditional, name string, isCM bool) bool {
	for _, v := range []*aretev1alpha1.VariantSpec{brc.Spec.WhenHealthy, brc.Spec.WhenEmpty, brc.Spec.WhenDegraded} {
		if v == nil {
			continue
		}
		if isCM && v.ManifestRef.ConfigMap != nil && v.ManifestRef.ConfigMap.Name == name {
			return true
		}
		if !isCM && v.ManifestRef.Secret != nil && v.ManifestRef.Secret.Name == name {
			return true
		}
	}
	return false
}

// SetupWithManager wires reconcile triggers:
//   - For()    — spec changes only (GenerationChangedPredicate); avoids
//     self-triggering on status patches.
//   - Watches BackupRepository — re-evaluate when source state changes.
//   - Watches ConfigMap / Secret — re-apply when manifest source edited.
//
// We deliberately do NOT Owns(materialized child): the child's GVK is
// arbitrary and not registered with the manager's scheme. Instead, the
// child has an ownerReference to the BRC, so K8s GC handles deletion.
// We re-reconcile on a slow cadence (controller-runtime default) plus
// the explicit watches above.
func (r *BackupRepositoryConditionalReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aretev1alpha1.BackupRepositoryConditional{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(
			&aretev1alpha1.BackupRepository{},
			handler.EnqueueRequestsFromMapFunc(r.mapBRToConditionals),
		).
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(r.mapManifestSourceToConditionals),
		).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.mapManifestSourceToConditionals),
		).
		Named("backuprepositoryconditional").
		Complete(r)
}
