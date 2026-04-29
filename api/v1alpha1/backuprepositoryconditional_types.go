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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupRepositoryConditionalSpec declares which manifest to materialize
// based on the state of a referenced BackupRepository. The state is
// derived from the BR's Healthy / observedInventory signals; one of three
// variants (whenHealthy, whenEmpty, whenDegraded) is chosen and the
// referenced manifest is server-side-applied with this CR as the owner.
//
// See ADR-023 for the design rationale; in short: bootstrap-mode-style
// decisions ("recovery vs initdb") need to be derived from observed
// repository state at deploy time, with stickiness against transient
// state flapping enforced by the target system's own immutability rules
// (e.g., cnpg's bootstrap.* fields are immutable after Cluster creation).
type BackupRepositoryConditionalSpec struct {
	// repositoryRef points to the cluster-scoped BackupRepository whose
	// state drives the variant selection.
	// +required
	RepositoryRef BackupRepositoryReference `json:"repositoryRef"`

	// whenHealthy is the variant materialized when BR.Healthy=True.
	// If nil, no resource is applied in this state.
	// +optional
	WhenHealthy *VariantSpec `json:"whenHealthy,omitempty"`

	// whenEmpty is the variant materialized when the BR has no successful
	// backups recorded (claimedLastSuccessfulBackup is nil OR
	// observedInventory.objectCount == 0). Typical use: an initdb-mode
	// fresh-bootstrap manifest.
	// If nil, no resource is applied in this state.
	// +optional
	WhenEmpty *VariantSpec `json:"whenEmpty,omitempty"`

	// whenDegraded is the variant materialized when the BR is neither
	// Healthy nor Empty (in-flight, broken, never-validated).
	// Typically left nil to refuse to materialize anything in uncertain
	// states — better to block deployment than to silently apply the
	// wrong manifest.
	// +optional
	WhenDegraded *VariantSpec `json:"whenDegraded,omitempty"`
}

// BackupRepositoryReference is a name-only reference to a cluster-scoped
// BackupRepository.
type BackupRepositoryReference struct {
	// name is the BackupRepository's metadata.name.
	// +required
	Name string `json:"name"`
}

// VariantSpec declares one option for materialization. The manifest YAML
// lives in a same-namespace ConfigMap or Secret; the controller reads
// that source, parses, and applies it (server-side) with this
// BackupRepositoryConditional set as controller owner.
type VariantSpec struct {
	// manifestRef points at a ConfigMap or Secret whose data[<key>] holds
	// the full manifest YAML to apply. Exactly one of configMap / secret
	// must be set.
	// +required
	ManifestRef ManifestRef `json:"manifestRef"`
}

// ManifestRef is a polymorphic reference to a manifest source. Same
// namespace as the BackupRepositoryConditional only — no cross-namespace
// reads (security boundary).
type ManifestRef struct {
	// configMap holds the manifest as plain text. Pair with
	// `kustomize configMapGenerator` to author the source as a normal
	// .yaml file with full IDE schema validation.
	// +optional
	ConfigMap *KeyedConfigMapReference `json:"configMap,omitempty"`

	// secret holds the manifest in a Secret data key. Use when the
	// manifest itself contains sensitive material (inline credentials,
	// pre-shared keys, etc.) and you don't want it readable to anyone
	// with ConfigMap access.
	// +optional
	Secret *KeyedSecretReference `json:"secret,omitempty"`
}

// KeyedConfigMapReference points at one key in a ConfigMap.
type KeyedConfigMapReference struct {
	// name is the ConfigMap's name (same namespace as the
	// BackupRepositoryConditional).
	// +required
	Name string `json:"name"`

	// key is the data[<key>] entry holding the manifest YAML.
	// +required
	Key string `json:"key"`
}

// KeyedSecretReference points at one key in a Secret.
type KeyedSecretReference struct {
	// name is the Secret's name (same namespace as the
	// BackupRepositoryConditional).
	// +required
	Name string `json:"name"`

	// key is the data[<key>] entry holding the manifest YAML.
	// +required
	Key string `json:"key"`
}

// AppliedResourceRef identifies the K8s resource currently materialized
// by this BackupRepositoryConditional.
type AppliedResourceRef struct {
	// +required
	APIVersion string `json:"apiVersion"`
	// +required
	Kind string `json:"kind"`
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// +required
	Name string `json:"name"`
}

// BackupRepositoryConditionalStatus is the observed state.
type BackupRepositoryConditionalStatus struct {
	// observedRepositoryState records which slot the controller picked
	// during the most recent reconcile: "healthy" | "empty" | "degraded"
	// | "" (no decision made yet).
	// +optional
	ObservedRepositoryState string `json:"observedRepositoryState,omitempty"`

	// decided is the variant slot that was applied: "whenHealthy" |
	// "whenEmpty" | "whenDegraded" | "" (no decision made yet).
	// +optional
	Decided string `json:"decided,omitempty"`

	// decidedAt is the timestamp of the most recent successful
	// materialization.
	// +optional
	DecidedAt *metav1.Time `json:"decidedAt,omitempty"`

	// appliedRef is the last resource the controller successfully
	// applied. nil until the first apply succeeds.
	// +optional
	AppliedRef *AppliedResourceRef `json:"appliedRef,omitempty"`

	// observedGeneration is the .metadata.generation this status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions report observability state. Standard types:
	//   - BackupRepositoryFound: the spec.repositoryRef resolves
	//   - ManifestSourceFound:   the chosen variant's ConfigMap/Secret exists
	//   - ManifestParsed:        the YAML deserialized into a valid object
	//   - ChildApplied:          server-side apply succeeded
	//   - Decided (rollup):      a variant was applied (or refused intentionally)
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Standard condition types for BackupRepositoryConditional.
const (
	BRConditionalBackupRepositoryFound = "BackupRepositoryFound"
	BRConditionalManifestSourceFound   = "ManifestSourceFound"
	BRConditionalManifestParsed        = "ManifestParsed"
	BRConditionalChildApplied          = "ChildApplied"
	BRConditionalDecided               = "Decided"
)

// Standard condition reasons for BackupRepositoryConditional.
const (
	BRConditionalReasonOK                  = "OK"
	BRConditionalReasonRepositoryNotFound  = "RepositoryNotFound"
	BRConditionalReasonNoVariantForState   = "NoVariantForState"
	BRConditionalReasonManifestSourceMiss  = "ManifestSourceMissing"
	BRConditionalReasonManifestParseFailed = "ManifestParseFailed"
	BRConditionalReasonApplyFailed         = "ApplyFailed"
	BRConditionalReasonImmutableConflict   = "ImmutableFieldRejected"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=brc
// +kubebuilder:printcolumn:name="Repository",type=string,JSONPath=`.spec.repositoryRef.name`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.observedRepositoryState`
// +kubebuilder:printcolumn:name="Decided",type=string,JSONPath=`.status.decided`
// +kubebuilder:printcolumn:name="Applied Kind",type=string,JSONPath=`.status.appliedRef.kind`
// +kubebuilder:printcolumn:name="Applied Name",type=string,JSONPath=`.status.appliedRef.name`
// +kubebuilder:printcolumn:name="Last Decided",type=date,JSONPath=`.status.decidedAt`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// BackupRepositoryConditional materializes one of three variant
// manifests based on the live state of a referenced BackupRepository.
// See ADR-023 for the bootstrap-mode-style use case (cnpg recovery vs
// initdb).
type BackupRepositoryConditional struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec is the desired state.
	// +required
	Spec BackupRepositoryConditionalSpec `json:"spec"`

	// status is the observed state.
	// +optional
	Status BackupRepositoryConditionalStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// BackupRepositoryConditionalList contains a list of BackupRepositoryConditional.
type BackupRepositoryConditionalList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []BackupRepositoryConditional `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BackupRepositoryConditional{}, &BackupRepositoryConditionalList{})
}
