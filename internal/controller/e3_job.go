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
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	aretev1alpha1 "github.com/TobiasHofmaenner/arete/api/v1alpha1"
)

const labelLevelE3 = "e3"

// shouldSpawnE3 returns true when E3 is enabled (sampledRetrievalInterval
// non-nil) AND the cycle has elapsed since the last completed E3 run.
func shouldSpawnE3(br *aretev1alpha1.BackupRepository, lastE3 *metav1.Time, now time.Time) bool {
	if br.Spec.SampledRetrievalInterval == nil {
		return false
	}
	if lastE3 == nil {
		return true
	}
	return now.Sub(lastE3.Time) >= br.Spec.SampledRetrievalInterval.Duration
}

// listE3Jobs returns all E3 Jobs for the BackupRepository.
func (r *BackupRepositoryReconciler) listE3Jobs(
	ctx context.Context, br *aretev1alpha1.BackupRepository,
) ([]batchv1.Job, error) {
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs,
		client.InNamespace(br.Spec.S3.CredentialsSecret.Namespace),
		client.MatchingLabels{
			labelOwnerName: br.Name,
			labelLevel:     labelLevelE3,
		},
	); err != nil {
		return nil, err
	}
	return jobs.Items, nil
}

// e3JobName produces a deterministic name unique per (BR, time-bucket).
func e3JobName(br *aretev1alpha1.BackupRepository) string {
	now := time.Now().UTC().Format("20060102-1504")
	hash := sha256.Sum256(fmt.Appendf(nil, "%s-e3-%d-%s", br.Name, br.Generation, now))
	short := hex.EncodeToString(hash[:4])
	base := fmt.Sprintf("arete-e3-%s", br.Name)
	if len(base) > 40 {
		base = base[:40]
	}
	return fmt.Sprintf("%s-%s", base, short)
}

// spawnE3Job creates an E3 sampled-retrieval Job. For wal-g, the
// controller pre-computes the random sample list (S3 LIST is cheaper
// than spawning a Job to do it); for restic the Job uses restic's own
// `--read-data-subset` random sampling.
func (r *BackupRepositoryReconciler) spawnE3Job(
	ctx context.Context, br *aretev1alpha1.BackupRepository, creds S3Credentials,
) (*batchv1.Job, error) {
	image, err := r.validatorImageFor(br.Spec.Format)
	if err != nil {
		return nil, err
	}

	cmd, args, env, err := r.e3CommandArgsEnv(ctx, br, creds)
	if err != nil {
		return nil, err
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      e3JobName(br),
			Namespace: br.Spec.S3.CredentialsSecret.Namespace,
			Labels: map[string]string{
				labelOwnerName: br.Name,
				labelLevel:     labelLevelE3,
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: ptrInt32(600),
			BackoffLimit:            ptrInt32(0),
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
						Env:             env,
					}},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(br, job, r.Scheme); err != nil {
		return nil, fmt.Errorf("set owner reference: %w", err)
	}
	if err := r.Create(ctx, job); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return job, nil
		}
		return nil, fmt.Errorf("create job: %w", err)
	}
	return job, nil
}

// e3CommandArgsEnv builds per-format Job invocation parameters.
func (r *BackupRepositoryReconciler) e3CommandArgsEnv(
	ctx context.Context, br *aretev1alpha1.BackupRepository, creds S3Credentials,
) (cmd, args []string, env []corev1.EnvVar, err error) {
	baseEnv := append(credentialEnvVars(br.Spec), e2EnvFor(br.Spec)...)
	target := int(*br.Spec.SampledRetrievalObjects)

	switch br.Spec.Format {
	case aretev1alpha1.BackupFormatWalg:
		samples, err := r.pickWalgSamples(ctx, br, creds, target)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("pick wal samples: %w", err)
		}
		if len(samples) == 0 {
			return nil, nil, nil, fmt.Errorf("no WAL segments to sample")
		}
		// Fetch each into a tmp file then delete; iterate. Use explicit
		// `if !` instead of `set -e` + `&&` because dash (ubuntu's /bin/sh)
		// silently continues on chain failures inside loops.
		script := "for s in " + strings.Join(samples, " ") + "; do " +
			"  echo fetching $s; " +
			"  if ! wal-g wal-fetch \"$s\" /tmp/seg; then " +
			"    echo FETCH_FAILED $s; exit 1; " +
			"  fi; " +
			"  rm -f /tmp/seg; " +
			"done; " +
			"echo OK " + fmt.Sprintf("%d", len(samples)) + " WAL segments verified"
		return []string{"sh", "-c"}, []string{script}, baseEnv, nil

	case aretev1alpha1.BackupFormatRestic:
		// restic --read-data-subset accepts size-bytes form; approximate
		// `target` packs as `target * 16M` (typical pack size). Bounded
		// cost; restic picks packs randomly until it hits the byte cap.
		approxBytes := target * 16
		subset := fmt.Sprintf("%dM", approxBytes)
		script := "export RESTIC_CACHE_DIR=/tmp && " +
			"restic check --read-data-subset " + subset
		return []string{"sh", "-c"}, []string{script}, baseEnv, nil
	}
	return nil, nil, nil, fmt.Errorf("E3 not implemented for format %q", br.Spec.Format)
}

// pickWalgSamples LISTs <prefix>/wal_005/ and returns N random WAL
// segment names (basename without extension, as wal-g wal-fetch expects).
func (r *BackupRepositoryReconciler) pickWalgSamples(
	ctx context.Context, br *aretev1alpha1.BackupRepository, creds S3Credentials, n int,
) ([]string, error) {
	mc, err := buildS3Client(br.Spec.S3, creds)
	if err != nil {
		return nil, err
	}
	listCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	listPrefix := br.Spec.S3.Prefix + "/wal_005/"
	var keys []string
	for obj := range mc.ListObjects(listCtx, br.Spec.S3.Bucket, minio.ListObjectsOptions{
		Prefix:    listPrefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	if len(keys) == 0 {
		return nil, nil
	}

	// Random N (with replacement allowed if N > len). We typically have
	// thousands of WAL segments and N is in the 10..1000 range, so
	// without-replacement sampling via shuffle is fine.
	if n > len(keys) {
		n = len(keys)
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	selected := keys[:n]

	// wal-g wal-fetch expects the segment name only — strip the prefix
	// AND any extension (.lzo / .br / etc. — wal-g handles compression
	// internally based on the writer's configured method).
	out := make([]string, 0, len(selected))
	for _, k := range selected {
		base := strings.TrimPrefix(k, listPrefix)
		if dot := strings.IndexByte(base, '.'); dot > 0 {
			base = base[:dot]
		}
		out = append(out, base)
	}
	sort.Strings(out) // deterministic order in the resulting Job's command line for log readability
	return out, nil
}
