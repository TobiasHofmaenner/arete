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
	"strconv"
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

const labelLevelE4 = "e4"

// e4ActiveDeadline caps a single E4 Job at 1 hour. A full restic check
// --read-data over a multi-GB repo or wal-g backup-fetch of a hot DB
// can take much longer than E2/E3's 10-minute window.
const e4ActiveDeadline = 1 * time.Hour

// shouldSpawnE4 returns true when E4 is enabled (fullRetrievalInterval
// non-nil) AND the cycle has elapsed since the last completed E4 run.
func shouldSpawnE4(br *aretev1alpha1.BackupRepository, lastE4 *metav1.Time, now time.Time) bool {
	if br.Spec.FullRetrievalInterval == nil {
		return false
	}
	if lastE4 == nil {
		return true
	}
	return now.Sub(lastE4.Time) >= br.Spec.FullRetrievalInterval.Duration
}

// listE4Jobs returns all E4 Jobs for the BackupRepository.
func (r *BackupRepositoryReconciler) listE4Jobs(
	ctx context.Context, br *aretev1alpha1.BackupRepository,
) ([]batchv1.Job, error) {
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs,
		client.InNamespace(br.Spec.S3.CredentialsSecret.Namespace),
		client.MatchingLabels{
			labelOwnerName: br.Name,
			labelLevel:     labelLevelE4,
		},
	); err != nil {
		return nil, err
	}
	return jobs.Items, nil
}

// e4JobName produces a deterministic name unique per (BR, time-bucket).
func e4JobName(br *aretev1alpha1.BackupRepository) string {
	now := time.Now().UTC().Format("20060102-1504")
	hash := sha256.Sum256(fmt.Appendf(nil, "%s-e4-%d-%s", br.Name, br.Generation, now))
	short := hex.EncodeToString(hash[:4])
	base := fmt.Sprintf("arete-e4-%s", br.Name)
	if len(base) > 40 {
		base = base[:40]
	}
	return fmt.Sprintf("%s-%s", base, short)
}

// e4PVCName returns the deterministic name of the PVC arete provisions
// for E4 retrievals on this BR. One PVC per BR — reused across runs;
// each run wipes /work at startup.
func e4PVCName(br *aretev1alpha1.BackupRepository) string {
	base := fmt.Sprintf("arete-e4-%s", br.Name)
	if len(base) > 50 {
		base = base[:50]
	}
	return base
}

// ensureE4PVC creates the BR's E4 PVC if it doesn't exist. Idempotent.
// Owned by the BR so it gets cleaned up when the BR is deleted.
func (r *BackupRepositoryReconciler) ensureE4PVC(
	ctx context.Context, br *aretev1alpha1.BackupRepository,
) error {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      e4PVCName(br),
			Namespace: br.Spec.S3.CredentialsSecret.Namespace,
			Labels: map[string]string{
				labelOwnerName: br.Name,
				labelLevel:     labelLevelE4,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: br.Spec.FullRetrievalStorageClass,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: *br.Spec.FullRetrievalPVCSize,
				},
			},
		},
	}
	if err := controllerutil.SetControllerReference(br, pvc, r.Scheme); err != nil {
		return fmt.Errorf("set pvc owner reference: %w", err)
	}
	if err := r.Create(ctx, pvc); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create pvc: %w", err)
	}
	return nil
}

// spawnE4Job creates an E4 full-retrieval Job. Provisions the PVC first
// if missing, then creates the Job that mounts it at /work.
func (r *BackupRepositoryReconciler) spawnE4Job(
	ctx context.Context, br *aretev1alpha1.BackupRepository,
) (*batchv1.Job, error) {
	image, err := r.validatorImageFor(br.Spec.Format)
	if err != nil {
		return nil, err
	}
	if err := r.ensureE4PVC(ctx, br); err != nil {
		return nil, err
	}

	cmd, args, env, err := r.e4CommandArgsEnv(br)
	if err != nil {
		return nil, err
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      e4JobName(br),
			Namespace: br.Spec.S3.CredentialsSecret.Namespace,
			Labels: map[string]string{
				labelOwnerName: br.Name,
				labelLevel:     labelLevelE4,
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: ptrInt32(600),
			BackoffLimit:            ptrInt32(0),
			ActiveDeadlineSeconds:   ptrInt64(int64(e4ActiveDeadline.Seconds())),
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
						Env:             env,
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "work",
							MountPath: "/work",
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "work",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: e4PVCName(br),
							},
						},
					}},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(br, job, r.Scheme); err != nil {
		return nil, fmt.Errorf("set job owner reference: %w", err)
	}
	if err := r.Create(ctx, job); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return job, nil
		}
		return nil, fmt.Errorf("create job: %w", err)
	}
	return job, nil
}

// e4CommandArgsEnv builds per-format Job invocation parameters. Each
// script wipes /work at startup, runs the format-specific full-retrieval
// command, and emits a final `STATS rc=… duration=… bytes=…` line that
// ingestE4Result parses into FullRetrievalStatus.
func (r *BackupRepositoryReconciler) e4CommandArgsEnv(
	br *aretev1alpha1.BackupRepository,
) (cmd, args []string, env []corev1.EnvVar, err error) {
	baseEnv := append(credentialEnvVars(br.Spec), e2EnvFor(br.Spec)...)

	switch br.Spec.Format {
	case aretev1alpha1.BackupFormatWalg:
		// Fetch the latest base into /work/extract; bytes = du of the
		// extracted directory (decompressed PG data dir). Throughput
		// includes the PVC write, which is what the user actually pays
		// for in a real restore — that's the number worth trending.
		script := "set -e; " +
			"rm -rf /work/extract /work/.wal-g; " +
			"mkdir -p /work/extract; " +
			"START=$(date +%s); " +
			"wal-g backup-fetch /work/extract LATEST 2>&1 | tail -10; " +
			"RC=$?; " +
			"END=$(date +%s); " +
			"DURATION=$((END - START)); " +
			"[ $DURATION -lt 1 ] && DURATION=1; " +
			"BYTES=$(du -sb /work/extract 2>/dev/null | awk '{print $1}'); " +
			"[ -z \"$BYTES\" ] && BYTES=0; " +
			"echo \"STATS rc=$RC duration=$DURATION bytes=$BYTES objects=1 failed=0\"; " +
			"exit $RC"
		return []string{"sh", "-c"}, []string{script}, baseEnv, nil

	case aretev1alpha1.BackupFormatRestic:
		// `stats --mode=raw-data` reports total repo bytes — captured
		// before the read so we can report throughput even though
		// `check --read-data` doesn't materialize everything to disk.
		// Then `check --read-data` does the actual full read.
		// --retry-lock to queue if E2 holds the exclusive repo lock.
		script := "set -e; " +
			"rm -rf /work/cache; " +
			"mkdir -p /work/cache; " +
			"export RESTIC_CACHE_DIR=/work/cache; " +
			"BYTES=$(restic --retry-lock 60s stats --mode=raw-data --json 2>/dev/null " +
			"| grep -oE '\"total_size\":[0-9]+' | awk -F: '{print $2}' | head -1); " +
			"[ -z \"$BYTES\" ] && BYTES=0; " +
			"START=$(date +%s); " +
			"restic --retry-lock 60s check --read-data 2>&1 | tail -10; " +
			"RC=$?; " +
			"END=$(date +%s); " +
			"DURATION=$((END - START)); " +
			"[ $DURATION -lt 1 ] && DURATION=1; " +
			"echo \"STATS rc=$RC duration=$DURATION bytes=$BYTES objects=1 failed=0\"; " +
			"exit $RC"
		return []string{"sh", "-c"}, []string{script}, baseEnv, nil
	}
	return nil, nil, nil, fmt.Errorf("E4 not implemented for format %q", br.Spec.Format)
}

// parseE4Stats extracts the FullRetrievalStatus from the validator's
// final "STATS rc=… duration=… bytes=… objects=… failed=…" line.
// Returns nil if no STATS line is present (validator died before emitting).
func parseE4Stats(logs string) *aretev1alpha1.FullRetrievalStatus {
	var statsLine string
	for line := range strings.SplitSeq(logs, "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "STATS ") {
			statsLine = t
		}
	}
	if statsLine == "" {
		return nil
	}
	fields := map[string]int64{}
	for tok := range strings.FieldsSeq(strings.TrimPrefix(statsLine, "STATS ")) {
		kv := strings.SplitN(tok, "=", 2)
		if len(kv) != 2 {
			continue
		}
		v, err := strconv.ParseInt(kv[1], 10, 64)
		if err != nil {
			continue
		}
		fields[kv[0]] = v
	}
	duration := fields["duration"]
	bytes := fields["bytes"]
	var throughput int64
	if duration > 0 {
		throughput = bytes / duration
	}
	return &aretev1alpha1.FullRetrievalStatus{
		DurationSeconds:       duration,
		BytesTransferred:      bytes,
		ThroughputBytesPerSec: throughput,
		ObjectsRetrieved:      fields["objects"],
		FailedObjects:         fields["failed"],
		TriggerSource:         aretev1alpha1.TriggerSourceScheduled,
	}
}
