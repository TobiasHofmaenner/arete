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

// ----- enums -----

// BackupFormat is the on-disk layout of the repository.
// +kubebuilder:validation:Enum=walg;restic;barman
type BackupFormat string

const (
	BackupFormatWalg   BackupFormat = "walg"
	BackupFormatRestic BackupFormat = "restic"
	BackupFormatBarman BackupFormat = "barman"
)

// TriggerSource records how a Job-spawned validation cycle was initiated.
// +kubebuilder:validation:Enum=scheduled;manual
type TriggerSource string

const (
	TriggerSourceScheduled TriggerSource = "scheduled"
	TriggerSourceManual    TriggerSource = "manual"
)

// ----- spec sub-types -----

// S3Source describes where the repository lives plus declared expectations
// about the bucket's security posture (verified by E1's BucketSecurityValid
// sub-condition).
type S3Source struct {
	// endpoint is the S3 API URL. HTTPS is mandatory (strict contract — no
	// field exists to override). Examples:
	//   https://s3.eu-central-1.amazonaws.com
	//   https://minio.example.internal
	// +kubebuilder:validation:Pattern=`^https://.+`
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

	// credentialsSecret references the primary Secret with at least
	// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (use keyMapping if your
	// Secret stores them under different names). Cluster-scoped CR
	// requires namespace.
	// +required
	CredentialsSecret SecretReference `json:"credentialsSecret"`

	// additionalSecrets supplies extra credential env vars sourced from
	// further Secrets — useful when format-specific encryption keys live
	// separately from the AWS auth Secret (e.g. cnpg-plugin-wal-g splits
	// nextcloud-s3 from nextcloud-walg-encryption). Each entry follows
	// the same SecretReference shape with optional keyMapping. Canonical
	// names already supplied by credentialsSecret take precedence.
	// +optional
	AdditionalSecrets []SecretReference `json:"additionalSecrets,omitempty"`

	// extraEnv is a verbatim map of name → value env vars passed to
	// validator Job containers. Use for non-secret configuration the
	// validator binary needs that the producer also sets — e.g.
	// WALG_COMPRESSION_METHOD=lzo when cnpg-plugin-wal-g writes lzo-
	// compressed segments.
	// +optional
	ExtraEnv map[string]string `json:"extraEnv,omitempty"`

	// requireObjectLock asserts that the bucket must have S3 Object Lock
	// configured. When true, BucketSecurityValid is False if the backend
	// does not report Object Lock as enabled. Opt-in.
	// +optional
	RequireObjectLock bool `json:"requireObjectLock,omitempty"`

	// requireBucketEncryption asserts that the bucket must have server-side
	// encryption (SSE-S3 or SSE-KMS) enabled. When true, BucketSecurityValid
	// is False if the backend does not report bucket encryption configured.
	// Opt-in.
	// +optional
	RequireBucketEncryption bool `json:"requireBucketEncryption,omitempty"`
}

// SecretReference is a namespaced reference to a Secret, with optional
// key remapping for Secrets whose key names don't match arete's canonical
// AWS_* / RESTIC_PASSWORD / WALG_LIBSODIUM_KEY conventions. Lets a tenant
// point arete at the producer's existing Secret (e.g. cnpg-plugin-wal-g's
// nextcloud-s3 with ACCESS_KEY_ID / ACCESS_SECRET_KEY) without copying.
type SecretReference struct {
	// +kubebuilder:validation:MinLength=1
	// +required
	Name string `json:"name"`

	// +kubebuilder:validation:MinLength=1
	// +required
	Namespace string `json:"namespace"`

	// keyMapping renames Secret data keys for arete's use. The MAP KEY is
	// the canonical name arete (and the validator binary) expects;
	// the MAP VALUE is the actual key in the Secret.
	//
	// Example — nextcloud-s3 has ACCESS_KEY_ID/ACCESS_SECRET_KEY:
	//   keyMapping:
	//     AWS_ACCESS_KEY_ID: ACCESS_KEY_ID
	//     AWS_SECRET_ACCESS_KEY: ACCESS_SECRET_KEY
	//
	// Canonical names not present in keyMapping use identity (key must
	// be the canonical name in the Secret). Unknown canonical names are
	// silently passed through with optional=true on the Job env var, so
	// missing optional creds (e.g. AWS_SESSION_TOKEN) don't fail the Job.
	// +optional
	KeyMapping map[string]string `json:"keyMapping,omitempty"`
}

// ----- spec -----

// BackupRepositorySpec defines the desired state of BackupRepository.
//
// All baseline fields (probe + metadata validation intervals, format,
// credentials, max backup lag) are required by design — arete enforces a
// strict contract (see ADR-023). E3 (sampled retrieval) and E4 (full
// retrieval) are opt-in by interval presence: setting the interval enables
// the level. There are no opt-out / pause / suspend / skip fields.
type BackupRepositorySpec struct {
	// s3 is the location of the repository plus declared bucket security
	// expectations.
	// +required
	S3 S3Source `json:"s3"`

	// format is the on-disk layout of the repository. Required — auto-detection
	// is intentionally not offered (declaring intent is part of the contract).
	// +required
	Format BackupFormat `json:"format"`

	// probeInterval is how often the E1 (existence) probe runs in the
	// controller process. Bounded to [1m, 1h].
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1m') && duration(self) <= duration('1h')",message="probeInterval must be between 1m and 1h"
	// +required
	ProbeInterval metav1.Duration `json:"probeInterval"`

	// metadataValidationInterval is how often the E2 (format-aware metadata)
	// validation Job runs (e.g. wal-g backup-list + wal-verify, restic check).
	// Bounded to [1h, 24h].
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1h') && duration(self) <= duration('24h')",message="metadataValidationInterval must be between 1h and 24h"
	// +required
	MetadataValidationInterval metav1.Duration `json:"metadataValidationInterval"`

	// sampledRetrievalInterval is how often the E3 (sampled retrieval) Job
	// runs. Optional — setting it enables E3. Bounded to [6h, 7d] when set.
	// +optional
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('6h') && duration(self) <= duration('168h')",message="sampledRetrievalInterval must be between 6h and 7d when set"
	SampledRetrievalInterval *metav1.Duration `json:"sampledRetrievalInterval,omitempty"`

	// sampledRetrievalObjects is the number of objects to download per E3
	// cycle. Fixed count (NOT a percent) keeps cost scale-invariant as the
	// repo grows. Required iff sampledRetrievalInterval is set. Bounded to
	// [10, 1000].
	// +optional
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=1000
	SampledRetrievalObjects *int32 `json:"sampledRetrievalObjects,omitempty"`

	// fullRetrievalInterval is how often the E4 (full retrieval) performance-
	// test Job runs. Optional — setting it enables E4. Bounded to [1d, 30d]
	// when set. Can also be triggered manually via the
	// arete.arete.io/trigger-full-retrieval annotation.
	// +optional
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('24h') && duration(self) <= duration('720h')",message="fullRetrievalInterval must be between 1d and 30d when set"
	FullRetrievalInterval *metav1.Duration `json:"fullRetrievalInterval,omitempty"`

	// fullRetrievalStorageClass is the StorageClass arete uses for the PVC
	// that streams E4 downloads. Should match the customer's actual restore
	// storage class so the throughput metric is an honest dress rehearsal.
	// Required iff fullRetrievalInterval is set.
	// +optional
	// +kubebuilder:validation:MinLength=1
	FullRetrievalStorageClass *string `json:"fullRetrievalStorageClass,omitempty"`

	// fullRetrievalPVCSize is the size of the PVC arete provisions for the
	// E4 Job. Sized for the largest single object only (per-chunk delete keeps
	// steady-state usage at one object's worth, regardless of repo size).
	// Required iff fullRetrievalInterval is set. Bounded to [1Gi, 100Gi].
	// +optional
	FullRetrievalPVCSize *resource.Quantity `json:"fullRetrievalPVCSize,omitempty"`

	// maxBackupLag is the maximum age of the most recent successful backup
	// before BackupCurrent flips to False. Bounded to [1h, 7d].
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1h') && duration(self) <= duration('168h')",message="maxBackupLag must be between 1h and 7d"
	// +required
	MaxBackupLag metav1.Duration `json:"maxBackupLag"`

	// expectedSizeBudget is an optional cap on observedInventory.totalBytes.
	// When set, SizeWithinBudget condition is emitted; when exceeded,
	// SizeWithinBudget is False with reason SizeBudgetExceeded.
	// +optional
	ExpectedSizeBudget *resource.Quantity `json:"expectedSizeBudget,omitempty"`
}

// ----- status sub-types -----

// LatestBackupStatus is the producer-claimed metadata of the most recent
// backup, parsed from format-specific sentinel files. NOT verified by arete.
type LatestBackupStatus struct {
	// name is the backup identifier as the producer wrote it (e.g. wal-g
	// BackupName, restic snapshot ID).
	Name string `json:"name"`

	// createdAt is the FinishTime parsed from the sentinel.
	CreatedAt metav1.Time `json:"createdAt"`

	// sizeBytes is the producer-reported compressed size.
	SizeBytes resource.Quantity `json:"sizeBytes"`

	// uncompressedSizeBytes is the producer-reported uncompressed size, if
	// available in the sentinel.
	// +optional
	UncompressedSizeBytes *resource.Quantity `json:"uncompressedSizeBytes,omitempty"`
}

// InventoryStatus is the arete-observed summary of repository contents
// (from S3 LIST). Lives under status.observedInventory.
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

// GrowthStatus tracks repository growth over a sliding window. Populated
// once arete has enough history to compute a delta (Pass 3-growth).
type GrowthStatus struct {
	// window is the time span this delta covers (e.g. 7d).
	Window metav1.Duration `json:"window"`

	// bytes is the increase in observedInventory.totalBytes over the window.
	Bytes resource.Quantity `json:"bytes"`

	// sampledAt is when the growth measurement completed.
	SampledAt metav1.Time `json:"sampledAt"`
}

// FullRetrievalStatus is the result of the most recent E4 Job — both pass/
// fail and a performance baseline. Throughput trended over time flags
// backend or network degradation BEFORE it becomes a recovery emergency.
type FullRetrievalStatus struct {
	// completedAt is when the E4 Job finished (regardless of success).
	CompletedAt metav1.Time `json:"completedAt"`

	// durationSeconds is total wall time for the retrieval.
	DurationSeconds int64 `json:"durationSeconds"`

	// bytesTransferred is the cumulative bytes read across all objects.
	BytesTransferred int64 `json:"bytesTransferred"`

	// throughputBytesPerSec is bytesTransferred / durationSeconds —
	// includes Ceph RBD write overhead because the PVC writes are real.
	// Plug this into RTO calculations.
	ThroughputBytesPerSec int64 `json:"throughputBytesPerSec"`

	// objectsRetrieved is the number of S3 objects successfully fetched.
	ObjectsRetrieved int64 `json:"objectsRetrieved"`

	// failedObjects is the number of GETs that errored mid-stream.
	FailedObjects int64 `json:"failedObjects"`

	// triggerSource records who started this run.
	TriggerSource TriggerSource `json:"triggerSource"`
}

// ----- condition + reason taxonomy -----

// Standard condition types emitted on BackupRepository. Each is binary
// (True | False | Unknown). No Warning tier — strict contract.
const (
	// E1 sub-conditions
	ConditionReachable           = "Reachable"
	ConditionBackupCurrent       = "BackupCurrent"
	ConditionBucketSecurityValid = "BucketSecurityValid"
	ConditionSizeWithinBudget    = "SizeWithinBudget"

	// E1 rollup
	ConditionProbeHealthy = "ProbeHealthy"

	// E2-E4 sub-conditions
	ConditionMetadataValid          = "MetadataValid"
	ConditionSampledIntegrityValid  = "SampledIntegrityValid"
	ConditionFullRetrievalCompleted = "FullRetrievalCompleted"

	// E2-E4 rollup
	ConditionValidationHealthy = "ValidationHealthy"

	// Overall rollup
	ConditionHealthy = "Healthy"
)

// Stable reason codes. CamelCase strings — used as Prometheus alert labels
// and condition.reason values. Do NOT add ad-hoc reason strings in code;
// add them here first.
const (
	// Success reasons
	ReasonProbeSucceeded = "ProbeSucceeded"

	// Setup / connectivity failures (E1)
	ReasonClientBuildFailed      = "ClientBuildFailed"
	ReasonCredentialsUnavailable = "CredentialsUnavailable"
	ReasonCredentialsRejected    = "CredentialsRejected"
	ReasonBucketNotFound         = "BucketNotFound"
	ReasonS3Unreachable          = "S3Unreachable"
	ReasonS3APIError             = "S3APIError"

	// Bucket security failures (E1)
	ReasonInsecureEndpoint          = "InsecureEndpoint"
	ReasonBucketPubliclyAccessible  = "BucketPubliclyAccessible"
	ReasonObjectLockMissing         = "ObjectLockMissing"
	ReasonBucketEncryptionMissing   = "BucketEncryptionMissing"
	ReasonBucketSecurityConfigDrift = "BucketSecurityConfigDrift"

	// Backup-state failures (E1)
	ReasonRepositoryEmpty    = "RepositoryEmpty"
	ReasonBackupLagExceeded  = "BackupLagExceeded"
	ReasonSizeBudgetExceeded = "SizeBudgetExceeded"

	// E2-E4 failures
	ReasonMetadataValidationFailed = "MetadataValidationFailed"
	ReasonSampledIntegrityFailed   = "SampledIntegrityFailed"
	ReasonFullRetrievalFailed      = "FullRetrievalFailed"
	ReasonValidatorImageMissing    = "ValidatorImageMissing"

	// Healthy=Unknown reasons (e.g. E2 not yet shipped, or first cycle pending)
	ReasonLayerTwoNotYetAvailable = "LayerTwoNotYetAvailable"
)

// ----- status -----

// BackupRepositoryStatus defines the observed state of BackupRepository.
//
// Field naming uses provenance prefixes: claimed* (producer self-reports
// parsed from sentinels — NOT verified by arete), observed* (arete
// measured directly via S3 API), verified* (arete proved via E2-E4
// validators). Conditions describe what passed and what didn't; the
// substructs hold the data behind those conditions.
type BackupRepositoryStatus struct {
	// observedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions reflect the current state of the repository. See the
	// Condition* constants in the api package for the enumerable types.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// lastProbedAt is when the most recent E1 probe completed.
	// +optional
	LastProbedAt *metav1.Time `json:"lastProbedAt,omitempty"`

	// claimedLatestBackup is the producer-reported metadata of the most
	// recent backup (parsed from format-specific sentinel files).
	// +optional
	ClaimedLatestBackup *LatestBackupStatus `json:"claimedLatestBackup,omitempty"`

	// claimedLastSuccessfulBackup is a convenience surface of
	// claimedLatestBackup.createdAt — the single most operationally
	// important field for "how long since the last backup."
	// +optional
	ClaimedLastSuccessfulBackup *metav1.Time `json:"claimedLastSuccessfulBackup,omitempty"`

	// detectedFormat is the on-disk format inferred from sentinel files
	// in the repository. Informational.
	// +optional
	DetectedFormat string `json:"detectedFormat,omitempty"`

	// detectedVersion is the producer-claimed version string parsed from
	// the sentinel (e.g. "sentinel-v2/pg-16.0.8" for wal-g). Informational
	// — the validator binary used at E2-E4 is always arete's pinned latest,
	// independent of detected version.
	// +optional
	DetectedVersion string `json:"detectedVersion,omitempty"`

	// observedInventory is arete's direct measurement of repository contents
	// (from S3 LIST). Recomputed at metadataValidationInterval cadence
	// (cheaper than per-probe). Stays nil until 3-inventory ships.
	// +optional
	ObservedInventory *InventoryStatus `json:"observedInventory,omitempty"`

	// observedGrowth is arete's measurement of inventory growth over a
	// rolling window. Stays nil until 3-growth ships.
	// +optional
	ObservedGrowth *GrowthStatus `json:"observedGrowth,omitempty"`

	// verifiedLastValidationAt is when the most recent successful E2 run
	// completed.
	// +optional
	VerifiedLastValidationAt *metav1.Time `json:"verifiedLastValidationAt,omitempty"`

	// verifiedLastSampledRetrievalAt is when the most recent E3 sampled
	// retrieval Job completed (success or failure — separate condition
	// SampledIntegrityValid carries the outcome).
	// +optional
	VerifiedLastSampledRetrievalAt *metav1.Time `json:"verifiedLastSampledRetrievalAt,omitempty"`

	// lastFullRetrieval is the result of the most recent E4 Job. Stays nil
	// until E4 has been enabled and run at least once.
	// +optional
	LastFullRetrieval *FullRetrievalStatus `json:"lastFullRetrieval,omitempty"`
}

// ----- root resource -----

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=backuprepo;br
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Format",type=string,JSONPath=`.spec.format`
// +kubebuilder:printcolumn:name="Health",type=string,JSONPath=`.status.conditions[?(@.type=="Healthy")].status`
// +kubebuilder:printcolumn:name="Probe",type=string,JSONPath=`.status.conditions[?(@.type=="ProbeHealthy")].status`
// +kubebuilder:printcolumn:name="Reachable",type=string,JSONPath=`.status.conditions[?(@.type=="Reachable")].status`
// +kubebuilder:printcolumn:name="Current",type=string,JSONPath=`.status.conditions[?(@.type=="BackupCurrent")].status`
// +kubebuilder:printcolumn:name="Last Backup",type=date,JSONPath=`.status.claimedLastSuccessfulBackup`
// +kubebuilder:printcolumn:name="Backup Size",type=string,JSONPath=`.status.claimedLatestBackup.sizeBytes`
// +kubebuilder:printcolumn:name="Last Probed",type=date,JSONPath=`.status.lastProbedAt`
// +kubebuilder:printcolumn:name="Validation",type=string,JSONPath=`.status.conditions[?(@.type=="ValidationHealthy")].status`
// +kubebuilder:printcolumn:name="Metadata",type=string,JSONPath=`.status.conditions[?(@.type=="MetadataValid")].status`
// +kubebuilder:printcolumn:name="Last Validated",type=date,JSONPath=`.status.verifiedLastValidationAt`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Detected",type=string,priority=1,JSONPath=`.status.detectedVersion`
// +kubebuilder:printcolumn:name="Objects",type=integer,priority=1,JSONPath=`.status.observedInventory.objectCount`
// +kubebuilder:printcolumn:name="Total Size",type=string,priority=1,JSONPath=`.status.observedInventory.totalBytes`
// +kubebuilder:printcolumn:name="Oldest",type=date,priority=1,JSONPath=`.status.observedInventory.oldestObject`
// +kubebuilder:printcolumn:name="Newest",type=date,priority=1,JSONPath=`.status.observedInventory.newestObject`
// +kubebuilder:printcolumn:name="Bucket Security",type=string,priority=1,JSONPath=`.status.conditions[?(@.type=="BucketSecurityValid")].status`
// +kubebuilder:printcolumn:name="Budget",type=string,priority=1,JSONPath=`.status.conditions[?(@.type=="SizeWithinBudget")].status`
// +kubebuilder:printcolumn:name="Sampled",type=string,priority=1,JSONPath=`.status.conditions[?(@.type=="SampledIntegrityValid")].status`
// +kubebuilder:printcolumn:name="E4",type=string,priority=1,JSONPath=`.status.conditions[?(@.type=="FullRetrievalCompleted")].status`
// +kubebuilder:printcolumn:name="E4 Throughput",type=integer,priority=1,JSONPath=`.status.lastFullRetrieval.throughputBytesPerSec`

// BackupRepository declares an S3-backed backup location to be continuously
// validated by the arete operator at four depth levels (E1 existence →
// E2 metadata → E3 sampled retrieval → E4 full retrieval). See ADR-022
// for the layer model and ADR-023 for arete's design.
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
