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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	aretev1alpha1 "github.com/TobiasHofmaenner/arete/api/v1alpha1"
)

// Labels arete puts on the Jobs it spawns. Used to find existing Jobs for
// a given BackupRepository + level on subsequent reconciles.
const (
	labelOwnerName = "arete.arete.io/backup-repository"
	labelLevel     = "arete.arete.io/level"
	labelLevelE2   = "e2"
)

// shouldSpawnE2 returns true if metadataValidationInterval has elapsed
// since the last verifiedLastValidationAt — or if no E2 has run at all.
func shouldSpawnE2(br *aretev1alpha1.BackupRepository, now time.Time) bool {
	if br.Status.VerifiedLastValidationAt == nil {
		return true
	}
	return now.Sub(br.Status.VerifiedLastValidationAt.Time) >=
		br.Spec.MetadataValidationInterval.Duration
}

// listE2Jobs returns all E2 Jobs the controller has spawned for the given
// BackupRepository (in any state — running, succeeded, failed, expired).
func (r *BackupRepositoryReconciler) listE2Jobs(
	ctx context.Context, br *aretev1alpha1.BackupRepository,
) ([]batchv1.Job, error) {
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs,
		client.InNamespace(br.Spec.S3.CredentialsSecret.Namespace),
		client.MatchingLabels{
			labelOwnerName: br.Name,
			labelLevel:     labelLevelE2,
		},
	); err != nil {
		return nil, err
	}
	return jobs.Items, nil
}

// jobInFlight reports whether a Job has neither completed nor failed
// terminally — covers "pod still pending" too, which jobActive (looking
// only at Status.Active) misses.
func jobInFlight(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		if c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed {
			return false
		}
	}
	return true
}

// jobSucceeded reports the Job completed with all containers exiting 0.
func jobSucceeded(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// jobFailed reports the Job exhausted retries and gave up.
func jobFailed(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// jobCompletionTime returns when a Job finished (success or failure).
// Falls back to creationTimestamp if completionTime is unset on a failed Job.
func jobCompletionTime(j *batchv1.Job) time.Time {
	if j.Status.CompletionTime != nil {
		return j.Status.CompletionTime.Time
	}
	return j.CreationTimestamp.Time
}

// pickLatestCompletedJob returns the most recently finished Job out of the
// given list, or nil if none have finished.
func pickLatestCompletedJob(jobs []batchv1.Job) *batchv1.Job {
	var done []batchv1.Job
	for _, j := range jobs {
		if jobSucceeded(&j) || jobFailed(&j) {
			done = append(done, j)
		}
	}
	if len(done) == 0 {
		return nil
	}
	sort.Slice(done, func(i, k int) bool {
		return jobCompletionTime(&done[i]).After(jobCompletionTime(&done[k]))
	})
	return &done[0]
}

// firstActiveJob returns the first in-flight Job, or nil. "In-flight"
// includes pods still pending — important to prevent spawning duplicates
// during the brief window between Create and the first status update.
func firstActiveJob(jobs []batchv1.Job) *batchv1.Job {
	for i := range jobs {
		if jobInFlight(&jobs[i]) {
			return &jobs[i]
		}
	}
	return nil
}

// spawnE2Job creates and submits an E2 metadata validation Job for the
// given BackupRepository. Job runs in the credentialsSecret's namespace
// (so it can mount the Secret directly) and is owner-referenced to the
// cluster-scoped BackupRepository (k8s GCs the Job when the BR is deleted).
func (r *BackupRepositoryReconciler) spawnE2Job(
	ctx context.Context, br *aretev1alpha1.BackupRepository,
) (*batchv1.Job, error) {
	image, err := r.validatorImageFor(br.Spec.Format)
	if err != nil {
		return nil, err
	}
	cmd, args := e2CommandFor(br.Spec.Format)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      e2JobName(br),
			Namespace: br.Spec.S3.CredentialsSecret.Namespace,
			Labels: map[string]string{
				labelOwnerName: br.Name,
				labelLevel:     labelLevelE2,
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: ptrInt32(600),
			BackoffLimit:            ptrInt32(0), // don't retry; spawn fresh next cycle
			ActiveDeadlineSeconds:   ptrInt64(int64(jobActiveDeadline.Seconds())),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:   corev1.RestartPolicyNever,
					SecurityContext: restrictedPodSecurityContext(),
					Containers: []corev1.Container{{
						Name:            "validator",
						Image:           image,
						Command:         cmd,
						Args:            args,
						SecurityContext: restrictedContainerSecurityContext(),
						EnvFrom: []corev1.EnvFromSource{{
							SecretRef: &corev1.SecretEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: br.Spec.S3.CredentialsSecret.Name,
								},
							},
						}},
						Env: e2EnvFor(br.Spec),
					}},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(br, job, r.Scheme); err != nil {
		return nil, fmt.Errorf("set owner reference: %w", err)
	}
	if err := r.Create(ctx, job); err != nil {
		// Race: previous reconcile already created this Job before its
		// status propagated. Treat as success — the Job exists, which is
		// what we wanted. Next reconcile will see it via the Owns watch.
		if apierrors.IsAlreadyExists(err) {
			return job, nil
		}
		return nil, fmt.Errorf("create job: %w", err)
	}
	return job, nil
}

// restrictedPodSecurityContext returns the PSS-restricted-compliant Pod-
// level SecurityContext required by namespaces that enforce the restricted
// Pod Security Standard.
func restrictedPodSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		RunAsNonRoot: ptrBool(true),
		RunAsUser:    ptrInt64(65532), // nonroot — works for any single-binary image
		RunAsGroup:   ptrInt64(65532),
		FSGroup:      ptrInt64(65532),
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		},
	}
}

// restrictedContainerSecurityContext returns the PSS-restricted-compliant
// container-level SecurityContext. ReadOnlyRootFilesystem intentionally
// not set: wal-g and restic both write temporary state during validation
// (decryption scratch, restic cache) and breaking that trades fewer false
// negatives for a marginal hardening gain. The other guards (no priv esc,
// no caps, non-root, runtimeDefault seccomp) are sufficient.
func restrictedContainerSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptrBool(false),
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		},
		RunAsNonRoot: ptrBool(true),
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		},
	}
}

// jobActiveDeadline caps a single Job at 10 minutes. A wal-g backup-list +
// wal-verify on a healthy repo finishes in seconds; restic check on a
// large repo can take a few minutes. 10m gives generous headroom and
// guarantees stuck Jobs get cleaned up before the next cycle.
const jobActiveDeadline = 10 * time.Minute

// e2JobName produces a deterministic name unique per (BR, time-bucket).
// Including a short hash of (BR generation + current minute) ensures
// successive cycles get distinct names and avoid collision with the
// previous Job that may still be in TTL grace.
func e2JobName(br *aretev1alpha1.BackupRepository) string {
	now := time.Now().UTC().Format("20060102-1504")
	hash := sha256.Sum256(fmt.Appendf(nil, "%s-%d-%s", br.Name, br.Generation, now))
	short := hex.EncodeToString(hash[:4])
	// Job name max 52 chars (k8s limit minus pod-name suffix headroom).
	base := fmt.Sprintf("arete-e2-%s", br.Name)
	if len(base) > 40 {
		base = base[:40]
	}
	return fmt.Sprintf("%s-%s", base, short)
}

// validatorImageFor returns the configured image for the format, or a
// loud error if the controller wasn't configured with one (chart bug —
// strict contract: refuse to silently substitute).
func (r *BackupRepositoryReconciler) validatorImageFor(format aretev1alpha1.BackupFormat) (string, error) {
	switch format {
	case aretev1alpha1.BackupFormatWalg:
		if r.ValidatorImages.Walg == "" {
			return "", fmt.Errorf("ARETE_VALIDATOR_WALG_IMAGE not configured")
		}
		return r.ValidatorImages.Walg, nil
	case aretev1alpha1.BackupFormatRestic:
		if r.ValidatorImages.Restic == "" {
			return "", fmt.Errorf("ARETE_VALIDATOR_RESTIC_IMAGE not configured")
		}
		return r.ValidatorImages.Restic, nil
	case aretev1alpha1.BackupFormatBarman:
		return "", fmt.Errorf("barman validator not yet implemented")
	}
	return "", fmt.Errorf("unknown format %q", format)
}

// e2CommandFor returns the (command, args) the Job runs for E2 validation
// per format.
func e2CommandFor(format aretev1alpha1.BackupFormat) ([]string, []string) {
	switch format {
	case aretev1alpha1.BackupFormatWalg:
		// wal-g validator image entrypoint is `wal-g`. Override with a
		// POSIX-sh chain so we can run backup-list + wal-verify in one Job.
		// (ubuntu:22.04's /bin/sh is dash — no `set -o pipefail`. The &&
		// already short-circuits on failure.)
		return []string{"sh", "-c"}, []string{
			"wal-g backup-list && wal-g wal-verify integrity",
		}
	case aretev1alpha1.BackupFormatRestic:
		// restic upstream image entrypoint is `restic`. We pass `check`
		// and rely on env vars (RESTIC_REPOSITORY, RESTIC_PASSWORD) from
		// the credentialsSecret.
		return nil, []string{"check"}
	}
	return nil, nil
}

// e2EnvFor returns the per-spec env vars (paths + region + endpoint) the
// validator needs in addition to the credentialsSecret-mounted env vars.
func e2EnvFor(spec aretev1alpha1.BackupRepositorySpec) []corev1.EnvVar {
	switch spec.Format {
	case aretev1alpha1.BackupFormatWalg:
		return []corev1.EnvVar{
			{Name: "WALG_S3_PREFIX", Value: fmt.Sprintf("s3://%s/%s", spec.S3.Bucket, spec.S3.Prefix)},
			{Name: "AWS_REGION", Value: spec.S3.Region},
			{Name: "AWS_ENDPOINT", Value: spec.S3.Endpoint},
			{Name: "AWS_S3_FORCE_PATH_STYLE", Value: "true"},
		}
	case aretev1alpha1.BackupFormatRestic:
		// restic uses RESTIC_REPOSITORY for the path. The user's
		// credentialsSecret should set RESTIC_PASSWORD plus AWS creds.
		return []corev1.EnvVar{
			{Name: "RESTIC_REPOSITORY",
				Value: fmt.Sprintf("s3:%s/%s/%s", spec.S3.Endpoint, spec.S3.Bucket, spec.S3.Prefix)},
			{Name: "AWS_REGION", Value: spec.S3.Region},
		}
	}
	return nil
}

// readJobOutput pulls the last 20 lines of the Job's pod logs. Used to
// surface a meaningful error message in the MetadataValid condition.
// On any failure, returns a generic placeholder rather than blocking the
// status update — log retrieval is best-effort.
func (r *BackupRepositoryReconciler) readJobOutput(
	ctx context.Context, job *batchv1.Job,
) string {
	if r.PodLogs == nil {
		return "(pod log retrieval not configured)"
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{"job-name": job.Name},
	); err != nil || len(pods.Items) == 0 {
		return "(no pod found for job)"
	}

	stream, err := r.PodLogs(ctx, job.Namespace, pods.Items[0].Name, 20)
	if err != nil {
		return fmt.Sprintf("(log retrieval failed: %s)", err.Error())
	}
	defer func() { _ = stream.Close() }()

	body, err := io.ReadAll(io.LimitReader(stream, 64*1024))
	if err != nil {
		return fmt.Sprintf("(log read failed: %s)", err.Error())
	}
	out := strings.TrimSpace(string(body))
	if out == "" {
		return "(empty pod logs)"
	}
	return out
}

// PodLogStreamer is the function arete uses to fetch pod logs. Swapped
// into the reconciler at startup with a kubernetes.Clientset-backed
// implementation so the controller package doesn't import client-go
// directly (kept testable).
type PodLogStreamer func(
	ctx context.Context, namespace, name string, tailLines int64,
) (io.ReadCloser, error)

// ----- helpers -----

func ptrInt32(v int32) *int32 { return &v }
func ptrInt64(v int64) *int64 { return &v }
func ptrBool(v bool) *bool    { return &v }
