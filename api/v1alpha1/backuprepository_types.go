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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupFormat is the on-disk layout of the repository.
// +kubebuilder:validation:Enum=walg;restic;barman
type BackupFormat string

const (
	BackupFormatWalg   BackupFormat = "walg"
	BackupFormatRestic BackupFormat = "restic"
	BackupFormatBarman BackupFormat = "barman"
)

// S3Source describes where the repository lives.
type S3Source struct {
	// endpoint is the S3 API URL (e.g., https://s3.eu-central-1.amazonaws.com,
	// or a MinIO endpoint). Must include scheme.
	// +kubebuilder:validation:Pattern=`^https?://.+`
	// +required
	Endpoint string `json:"endpoint"`

	// region is the S3 region.
	// +kubebuilder:validation:MinLength=1
	// +required
	Region string `json:"region"`

	// bucket is the S3 bucket name.
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=63
	// +required
	Bucket string `json:"bucket"`

	// prefix is the key prefix that scopes this repository within the bucket.
	// Must NOT start or end with "/".
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[^/].*[^/]$|^[^/]$`
	// +required
	Prefix string `json:"prefix"`

	// credentialsSecret references a Secret containing AWS_ACCESS_KEY_ID
	// and AWS_SECRET_ACCESS_KEY keys. Cluster-scoped CR requires namespace.
	// +required
	CredentialsSecret SecretReference `json:"credentialsSecret"`
}

// SecretReference is a namespaced reference to a Secret.
type SecretReference struct {
	// +kubebuilder:validation:MinLength=1
	// +required
	Name string `json:"name"`

	// +kubebuilder:validation:MinLength=1
	// +required
	Namespace string `json:"namespace"`
}

// BackupRepositorySpec defines the desired state of BackupRepository.
//
// All fields are required by design — arete enforces a strict contract
// (see ADR-023). There are no optional best-practice fields, no opt-out
// flags, and no escape hatches. If a CR exists, the contract is in force.
type BackupRepositorySpec struct {
	// s3 is the location of the repository.
	// +required
	S3 S3Source `json:"s3"`

	// format is the on-disk layout of the repository. Required — auto-detection
	// is intentionally not offered (declaring intent is part of the contract).
	// +required
	Format BackupFormat `json:"format"`

	// probeInterval is how often Layer-1 (format-agnostic S3 reachability)
	// is checked. Bounded to [1m, 1h] — values outside this range render the
	// contract meaningless and are rejected.
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1m') && duration(self) <= duration('1h')",message="probeInterval must be between 1m and 1h"
	// +required
	ProbeInterval metav1.Duration `json:"probeInterval"`

	// validationInterval is how often Layer-2 (format-aware) validation runs
	// (e.g. wal-g backup-list, restic check). Bounded to [1h, 24h].
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1h') && duration(self) <= duration('24h')",message="validationInterval must be between 1h and 24h"
	// +required
	ValidationInterval metav1.Duration `json:"validationInterval"`
}

// InventoryStatus is a summary of repository contents (rendered as a small
// at-a-glance overview; full per-object detail lives in Prometheus).
type InventoryStatus struct {
	// objectCount is the total number of S3 objects under spec.s3.prefix.
	ObjectCount int64 `json:"objectCount"`

	// totalBytes is the cumulative size of all objects.
	TotalBytes resource.Quantity `json:"totalBytes"`

	// oldestObject is the LastModified of the earliest object.
	// +optional
	OldestObject *metav1.Time `json:"oldestObject,omitempty"`

	// newestObject is the LastModified of the latest object.
	// +optional
	NewestObject *metav1.Time `json:"newestObject,omitempty"`
}

// Standard condition types emitted on BackupRepository.
const (
	// ConditionReachable: True iff the most recent Layer-1 probe succeeded.
	ConditionReachable = "Reachable"
	// ConditionHealthy: True iff the most recent Layer-2 validation succeeded.
	// Soft-fail / "warning" tier intentionally not offered.
	ConditionHealthy = "Healthy"
)

// BackupRepositoryStatus defines the observed state of BackupRepository.
type BackupRepositoryStatus struct {
	// observedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions reflect the current state of the repository.
	// Only Reachable and Healthy are emitted; both are binary (True | False
	// | Unknown). No Warning tier — see ADR-023 strict-contract rule.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// lastProbedAt is when the most recent Layer-1 probe completed.
	// +optional
	LastProbedAt *metav1.Time `json:"lastProbedAt,omitempty"`

	// lastValidatedAt is when the most recent Layer-2 validation completed.
	// +optional
	LastValidatedAt *metav1.Time `json:"lastValidatedAt,omitempty"`

	// inventory summarises the current contents of the repository.
	// +optional
	Inventory *InventoryStatus `json:"inventory,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=backuprepo;br
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Format",type=string,JSONPath=`.spec.format`
// +kubebuilder:printcolumn:name="Reachable",type=string,JSONPath=`.status.conditions[?(@.type=="Reachable")].status`
// +kubebuilder:printcolumn:name="Healthy",type=string,JSONPath=`.status.conditions[?(@.type=="Healthy")].status`
// +kubebuilder:printcolumn:name="Objects",type=integer,JSONPath=`.status.inventory.objectCount`
// +kubebuilder:printcolumn:name="Last Probed",type=date,JSONPath=`.status.lastProbedAt`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// BackupRepository declares an S3-backed backup location to be continuously
// probed (Layer 1) and format-validated (Layer 2) by the arete operator.
type BackupRepository struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec BackupRepositorySpec `json:"spec"`

	// +optional
	Status BackupRepositoryStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// BackupRepositoryList contains a list of BackupRepository.
type BackupRepositoryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []BackupRepository `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BackupRepository{}, &BackupRepositoryList{})
}
