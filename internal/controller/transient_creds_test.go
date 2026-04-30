/*
Copyright 2026 Tobias Hofmaenner.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	aretev1alpha1 "github.com/TobiasHofmaenner/arete/api/v1alpha1"
)

func updateEvent(old, new client.Object) event.UpdateEvent {
	return event.UpdateEvent{ObjectOld: old, ObjectNew: new}
}

func createEvent(o client.Object) event.CreateEvent {
	return event.CreateEvent{Object: o}
}

func deleteEvent(o client.Object) event.DeleteEvent {
	return event.DeleteEvent{Object: o}
}

func genericEvent(o client.Object) event.GenericEvent {
	return event.GenericEvent{Object: o}
}

// Pure-Go unit tests for the credential / annotation / condition
// helpers added in v0.5.21. Don't need envtest — they exercise the
// branches the controller depends on for transient-tolerance and
// force-revalidate handling.

func TestErrCredentialsSecretNotFound_Sentinel(t *testing.T) {
	// resolveCredentials wraps with %w so callers detect via errors.Is.
	wrapped := errors.New("wrapped: " + ErrCredentialsSecretNotFound.Error())
	if errors.Is(wrapped, ErrCredentialsSecretNotFound) {
		t.Fatal("plain string concat must NOT match Is — only %%w should")
	}
}

func TestResolveForceRevalidate_Empty(t *testing.T) {
	r := &BackupRepositoryReconciler{}
	br := &aretev1alpha1.BackupRepository{}
	_, ok := r.resolveForceRevalidate(br)
	if ok {
		t.Fatal("expected ok=false when annotation absent")
	}
}

func TestResolveForceRevalidate_FreshAnnotation(t *testing.T) {
	r := &BackupRepositoryReconciler{}
	now := time.Now().UTC().Truncate(time.Second)
	br := &aretev1alpha1.BackupRepository{}
	br.SetAnnotations(map[string]string{
		aretev1alpha1.AnnotationForceRevalidate: now.Format(time.RFC3339),
	})

	got, ok := r.resolveForceRevalidate(br)
	if !ok {
		t.Fatal("expected ok=true for fresh annotation, no prior status")
	}
	if !got.Equal(now) {
		t.Errorf("expected %v, got %v", now, got)
	}
}

func TestResolveForceRevalidate_AlreadyHonored(t *testing.T) {
	r := &BackupRepositoryReconciler{}
	ts := time.Now().UTC().Truncate(time.Second)
	br := &aretev1alpha1.BackupRepository{}
	br.SetAnnotations(map[string]string{
		aretev1alpha1.AnnotationForceRevalidate: ts.Format(time.RFC3339),
	})
	already := metav1.NewTime(ts)
	br.Status.LastForceRevalidatedAt = &already

	if _, ok := r.resolveForceRevalidate(br); ok {
		t.Fatal("must not honor an annotation we've already recorded as applied")
	}
}

func TestResolveForceRevalidate_OlderThanLast(t *testing.T) {
	r := &BackupRepositoryReconciler{}
	older := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Second)
	newer := time.Now().UTC().Truncate(time.Second)
	br := &aretev1alpha1.BackupRepository{}
	br.SetAnnotations(map[string]string{
		aretev1alpha1.AnnotationForceRevalidate: older.Format(time.RFC3339),
	})
	last := metav1.NewTime(newer)
	br.Status.LastForceRevalidatedAt = &last

	if _, ok := r.resolveForceRevalidate(br); ok {
		t.Fatal("must reject annotation older than lastForceRevalidatedAt")
	}
}

func TestResolveForceRevalidate_RFC3339Nano(t *testing.T) {
	r := &BackupRepositoryReconciler{}
	now := time.Now().UTC()
	br := &aretev1alpha1.BackupRepository{}
	br.SetAnnotations(map[string]string{
		aretev1alpha1.AnnotationForceRevalidate: now.Format(time.RFC3339Nano),
	})
	if _, ok := r.resolveForceRevalidate(br); !ok {
		t.Fatal("RFC3339Nano (kubectl annotate $(date)) must be accepted")
	}
}

func TestResolveForceRevalidate_Garbage(t *testing.T) {
	r := &BackupRepositoryReconciler{}
	br := &aretev1alpha1.BackupRepository{}
	br.SetAnnotations(map[string]string{
		aretev1alpha1.AnnotationForceRevalidate: "not-a-timestamp",
	})
	if _, ok := r.resolveForceRevalidate(br); ok {
		t.Fatal("malformed annotation must be silently ignored, not honored")
	}
}

func TestPreservedCondition_RealResultPreserved(t *testing.T) {
	br := &aretev1alpha1.BackupRepository{}
	br.Status.Conditions = []metav1.Condition{{
		Type:    aretev1alpha1.ConditionMetadataValid,
		Status:  metav1.ConditionTrue,
		Reason:  aretev1alpha1.ReasonProbeSucceeded,
		Message: "validator exit 0",
	}}
	got := preservedCondition(br, aretev1alpha1.ConditionMetadataValid)
	if got == nil {
		t.Fatal("expected the True/ProbeSucceeded condition to be preserved across a credential gap")
	}
	if got.Status != metav1.ConditionTrue {
		t.Errorf("expected ConditionTrue, got %v", got.Status)
	}
}

func TestPreservedCondition_LayerTwoNotYetIsTreatedAsAbsent(t *testing.T) {
	// LayerTwoNotYetAvailable means "we have no real result yet" — it's
	// not a verifiable signal worth preserving.
	br := &aretev1alpha1.BackupRepository{}
	br.Status.Conditions = []metav1.Condition{{
		Type:   aretev1alpha1.ConditionMetadataValid,
		Status: metav1.ConditionUnknown,
		Reason: aretev1alpha1.ReasonLayerTwoNotYetAvailable,
	}}
	got := preservedCondition(br, aretev1alpha1.ConditionMetadataValid)
	if got != nil {
		t.Fatal("LayerTwoNotYetAvailable must be treated as absent, not preserved")
	}
}

func TestPreservedCondition_AbsentIsNil(t *testing.T) {
	br := &aretev1alpha1.BackupRepository{}
	if got := preservedCondition(br, aretev1alpha1.ConditionMetadataValid); got != nil {
		t.Fatal("expected nil when condition is absent")
	}
}

func TestIsHealthyUnknown(t *testing.T) {
	cases := []struct {
		name string
		cs   []metav1.Condition
		want bool
	}{
		{"absent", nil, false},
		{"true", []metav1.Condition{{Type: aretev1alpha1.ConditionHealthy, Status: metav1.ConditionTrue}}, false},
		{"false", []metav1.Condition{{Type: aretev1alpha1.ConditionHealthy, Status: metav1.ConditionFalse}}, false},
		{"unknown", []metav1.Condition{{Type: aretev1alpha1.ConditionHealthy, Status: metav1.ConditionUnknown}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			br := &aretev1alpha1.BackupRepository{}
			br.Status.Conditions = tc.cs
			if got := isHealthyUnknown(br); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestForceRevalidatePredicate_OnlyAnnotationDelta(t *testing.T) {
	// The predicate must distinguish "annotation changed" from "any
	// other metadata change" so we don't get spammed by label edits.
	old := &aretev1alpha1.BackupRepository{}
	old.SetAnnotations(map[string]string{aretev1alpha1.AnnotationForceRevalidate: "2026-01-01T00:00:00Z"})
	new1 := old.DeepCopy()
	new2 := old.DeepCopy()
	new2.SetAnnotations(map[string]string{aretev1alpha1.AnnotationForceRevalidate: "2026-02-01T00:00:00Z"})

	pred := forceRevalidatePredicate{}
	if pred.Update(updateEvent(old, new1)) {
		t.Error("identical annotation must NOT trigger reconcile")
	}
	if !pred.Update(updateEvent(old, new2)) {
		t.Error("annotation value change MUST trigger reconcile")
	}
	if pred.Create(createEvent(new1)) || pred.Delete(deleteEvent(new1)) || pred.Generic(genericEvent(new1)) {
		t.Error("non-update events should not be filtered through this predicate")
	}
}
