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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aretev1alpha1 "github.com/TobiasHofmaenner/arete/api/v1alpha1"
)

var _ = Describe("BackupRepository Controller", func() {
	Context("API validation", func() {
		ctx := context.Background()

		valid := func(name string) *aretev1alpha1.BackupRepository {
			return &aretev1alpha1.BackupRepository{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: aretev1alpha1.BackupRepositorySpec{
					S3: aretev1alpha1.S3Source{
						Endpoint: "https://s3.example.com",
						Region:   "eu-central-1",
						Bucket:   "test-bucket",
						Prefix:   "test/prefix",
						CredentialsSecret: aretev1alpha1.SecretReference{
							Name:      "test-creds",
							Namespace: "default",
						},
					},
					Format:                     aretev1alpha1.BackupFormatRestic,
					ProbeInterval:              metav1.Duration{Duration: 5 * time.Minute},
					MetadataValidationInterval: metav1.Duration{Duration: 6 * time.Hour},
					MaxBackupLag:               metav1.Duration{Duration: 25 * time.Hour},
				},
			}
		}

		It("accepts a valid object, including harmless extraEnv", func() {
			br := valid("validation-ok")
			br.Spec.S3.ExtraEnv = map[string]string{"WALG_COMPRESSION_METHOD": "lzo"}
			Expect(k8sClient.Create(ctx, br)).To(Succeed())
			Expect(k8sClient.Delete(ctx, br)).To(Succeed())
		})

		It("rejects sampled retrieval without an object count", func() {
			br := valid("validation-e3")
			br.Spec.SampledRetrievalInterval = &metav1.Duration{Duration: 6 * time.Hour}
			err := k8sClient.Create(ctx, br)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("sampledRetrievalObjects is required"))
		})

		It("rejects scheduled full retrieval without a storage class and size", func() {
			br := valid("validation-e4")
			br.Spec.FullRetrievalInterval = &metav1.Duration{Duration: 48 * time.Hour}
			err := k8sClient.Create(ctx, br)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("fullRetrievalStorageClass and fullRetrievalPVCSize are required"))
		})

		It("rejects extraEnv that would run a command or repoint the validator", func() {
			for _, name := range []string{"RESTIC_PASSWORD_COMMAND", "LD_PRELOAD", "WALG_FILE_PREFIX"} {
				br := valid("validation-env")
				br.Spec.S3.ExtraEnv = map[string]string{name: "x"}
				err := k8sClient.Create(ctx, br)
				Expect(err).To(HaveOccurred(), name)
				Expect(err.Error()).To(ContainSubstring("extraEnv must not set"), name)
			}
		})

		It("rejects a plain-http endpoint", func() {
			br := valid("validation-http")
			br.Spec.S3.Endpoint = "http://s3.example.com"
			Expect(k8sClient.Create(ctx, br)).NotTo(Succeed())
		})
	})

	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		// Cluster-scoped — no namespace.
		typeNamespacedName := types.NamespacedName{Name: resourceName}

		BeforeEach(func() {
			By("creating the custom resource for the Kind BackupRepository")
			existing := &aretev1alpha1.BackupRepository{}
			err := k8sClient.Get(ctx, typeNamespacedName, existing)
			if err != nil && errors.IsNotFound(err) {
				resource := &aretev1alpha1.BackupRepository{
					ObjectMeta: metav1.ObjectMeta{Name: resourceName},
					Spec: aretev1alpha1.BackupRepositorySpec{
						S3: aretev1alpha1.S3Source{
							Endpoint: "https://s3.example.com",
							Region:   "eu-central-1",
							Bucket:   "test-bucket",
							Prefix:   "test/prefix",
							CredentialsSecret: aretev1alpha1.SecretReference{
								Name:      "test-creds",
								Namespace: "default",
							},
						},
						Format:                     aretev1alpha1.BackupFormatWalg,
						ProbeInterval:              metav1.Duration{Duration: 5 * time.Minute},
						MetadataValidationInterval: metav1.Duration{Duration: 6 * time.Hour},
						MaxBackupLag:               metav1.Duration{Duration: 25 * time.Hour},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &aretev1alpha1.BackupRepository{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance BackupRepository")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &BackupRepositoryReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
