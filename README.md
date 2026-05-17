# arete

> Kubernetes operator for S3 backup repository validation.

Greek ἀρετή — *excellence at fulfilling one's purpose*. A backup's *arete* is its restorability.

---

## Status

Alpha, but functionally complete. As of v0.5.x:

- E1–E4 validation all shipped and running in production against real wal-g + restic repositories
- `BackupRepository` (core) and `BackupRepositoryConditional` (the consumer-side decision driver) CRDs both live
- Prometheus metrics, PrometheusRule alerts, and a Grafana dashboard ship with the chart
- Per-format validators bundled as separate container images (`arete-validator-walg`, `arete-validator-restic`)

Format coverage today: **wal-g**, **restic**. Others (e.g. `barman-cloud`) can be added by following the same pattern (an image + per-format job command builder) without touching core controller logic.

## What it does

For each `BackupRepository` you declare, arete validates the repository at four depth levels (E1–E4). Each level has its own cadence and its own status condition; higher levels are more thorough and more expensive.

| Level | What | Cost | Default |
|---|---|---|---|
| **E1** | Existence — bucket+prefix reachable, sentinel files present, recent backup within `maxBackupLag`, observed inventory (object count + total bytes + oldest/newest) recorded | Trivial (S3 LIST + a few HEADs) | Always on (`probeInterval`) |
| **E2** | Metadata validation — the format's own validator accepts the metadata as well-formed (`wal-g backup-list`, `restic check`) | Low (small reads) | Always on (`metadataValidationInterval`) |
| **E3** | Sampled retrieval — random subset of objects fully downloaded, decrypted, and verified. **Bounded by fixed object count, not percent**, so cost stays scale-invariant | Bandwidth: bounded by `sampledRetrievalObjects` | Opt-in (`sampledRetrievalInterval`) |
| **E4** | Full retrieval — full restore-grade read written to a customer-specified PVC, throughput measured. Doubles as RTO performance baseline | Bandwidth: 100% of repo | Opt-in (`fullRetrievalInterval`) |

Beyond per-level validation, arete:

- **Provides a unified inventory and health overview** of every backup repository in the cluster — `kubectl get br` shows status of E2/E3/E4 explicitly per repo
- **Exposes Prometheus metrics**: `arete_validation_runs_total`, `arete_validation_duration_seconds`, `arete_e4_throughput_bytes_per_sec`, `arete_backup_age_seconds`, `arete_inventory_objects`, `arete_inventory_bytes`, `arete_condition_state`
- **Ships a PrometheusRule** with alerts for unhealthy BRs, stale backups, validation halt, and E4 throughput regression
- **Ships a Grafana dashboard** consuming all of the above
- **Optionally drives downstream resources** via `BackupRepositoryConditional` — pick one of several manifests based on observed BR state (e.g., a Postgres cluster bootstraps via recovery vs initdb depending on whether backups exist)

### Status field provenance

Every status field is prefixed by what arete actually knows:

- `claimed*` — what the producer self-reports (read from sentinel files, never independently verified)
- `observed*` — what arete measured directly via S3 API (object counts, byte totals, last-modified)
- `verified*` — what arete proved by running a validator (E2 metadata parse, E3 sample fetch, E4 full retrieval)

So consumers can tell what's authoritative vs. what's just claimed.

### Validator strategy: always latest, pinned per arete release

arete ships **one validator container image per format**, pinned to the latest version of each tool per arete release. There is no per-version catalog; there is no per-tenant override. This is intentional — it makes arete double as a **forward-compatibility canary**: if a future arete release bumps the validator and validation starts failing against existing repos, you learn about the upcoming incompatibility *before* upgrading your production producer.

### Strict contract

If arete is running, your backups are in one of three states:
1. Safe.
2. arete is loudly screaming they aren't.
3. arete itself is not running (observable from outside).

There is no fourth state. No `ignoreErrors`, no `skipMetadataValidation`, no `pause`/`suspend`, no soft-fail tier on `Healthy`. If you don't want guarding, delete the CR.

## Scope

In scope:
- S3 / S3-compatible backup repositories (MinIO, AWS S3, Ceph RGW, R2, B2, etc.)
- Format-aware validation for `wal-g` and `restic` today (`barman-cloud` and others extensible)
- E1–E4 monitoring with Prometheus metrics + Grafana dashboard
- Conditional materialization of downstream resources via `BackupRepositoryConditional`

Out of scope:
- Backup creation (delegated to existing tools — cnpg-plugin-wal-g, VolSync, etc.)
- Backup restoration (delegated to existing tools)
- Retention enforcement (delegated to S3 lifecycle rules / object lock)
- DR drill orchestration (delegated to workflow engines)
- Non-S3 storage backends

## CRDs

### `BackupRepository` (cluster-scoped)

The core CR. Represents one S3 prefix, monitored at all enabled levels.

```yaml
apiVersion: arete.arete.io/v1alpha1
kind: BackupRepository
metadata:
  name: my-postgres-backups
spec:
  s3:
    endpoint: https://s3.example.com
    region: us-east-1
    bucket: backups
    prefix: customer/postgres/16
    credentialsSecret:
      name: my-s3-creds
      namespace: customer
      keyMapping:                       # remap canonical names → actual Secret keys
        AWS_ACCESS_KEY_ID: ACCESS_KEY_ID
        AWS_SECRET_ACCESS_KEY: ACCESS_SECRET_KEY
    additionalSecrets:                  # extra credential sources (e.g. encryption keys)
      - name: my-walg-encryption
        namespace: customer
        keyMapping:
          WALG_LIBSODIUM_KEY: libsodiumKey
    extraEnv:                           # non-secret format config (passed to validator)
      WALG_COMPRESSION_METHOD: brotli
      WALG_LIBSODIUM_KEY_TRANSFORM: hex
  format: walg                          # walg | restic

  # Per-level cadence (presence enables that level)
  probeInterval: 5m                     # E1 (required)
  metadataValidationInterval: 6h        # E2 (required)
  sampledRetrievalInterval: 6h          # E3 (optional)
  sampledRetrievalObjects: 10           # E3 sample size, scale-invariant
  fullRetrievalInterval: 7d             # E4 (optional)
  fullRetrievalStorageClass: ceph-bdr   # E4 PVC class (required iff E4 enabled)
  fullRetrievalPVCSize: 10Gi            # E4 PVC size (required iff E4 enabled)

  # SLO knobs
  maxBackupLag: 25h
  expectedSizeBudget: 100Gi             # optional → SizeWithinBudget condition
```

Status surfaces conditions (`Reachable`, `BucketSecurityValid`, `BackupCurrent`, `MetadataValid`, `SampledIntegrityValid`, `FullRetrievalCompleted`, `SizeWithinBudget`, plus rollups `ProbeHealthy`, `ValidationHealthy`, `Healthy`) and structured fields under `claimedLatestBackup`, `observedInventory`, `lastFullRetrieval`, etc.

`kubectl get br` default columns:

```
NAME   FORMAT  HEALTH  PROBE  REACHABLE  CURRENT  LAST BACKUP  BACKUP SIZE  LAST PROBED
       E2  E3  E4  LAST VALIDATED  AGE
```

A blank cell in `E2`/`E3`/`E4` means that layer isn't enabled for this BR — different from `False` (enabled and failing).

### `BackupRepositoryConditional` (namespace-scoped)

Materializes one of several variant manifests based on the live state of a referenced `BackupRepository`. Each variant is a complete K8s manifest stored in a same-namespace ConfigMap or Secret. arete reads the chosen variant, parses, server-side-applies it with the BRC as `ownerReference`, and tracks state.

```yaml
apiVersion: arete.arete.io/v1alpha1
kind: BackupRepositoryConditional
metadata:
  name: nextcloud-cnpg-bootstrap
  namespace: customer
spec:
  repositoryRef:
    name: customer-nextcloud-postgres
  whenHealthy:
    manifestRef:
      configMap:
        name: nextcloud-cnpg-cluster-recovery
        key: cluster.yaml
  whenEmpty:
    manifestRef:
      configMap:
        name: nextcloud-cnpg-cluster-init
        key: cluster.yaml
  # whenDegraded omitted → refuse to materialize anything in uncertain states
status:
  observedRepositoryState: healthy        # healthy | empty | degraded
  decided: whenHealthy                    # which slot was applied
  appliedRef:
    apiVersion: postgresql.cnpg.io/v1
    kind: Cluster
    name: nextcloud-cnpg-main
    namespace: customer
  conditions: [BackupRepositoryFound, ManifestSourceFound, ManifestParsed,
               ChildApplied, Decided]
```

State derivation:
- **`empty`** — no successful backup recorded AND `observedInventory.objectCount == 0` (catches fresh tenants unambiguously)
- **`healthy`** — BR's `Healthy` rollup condition is True
- **`degraded`** — anything else (broken, in-flight, never-validated)

Stickiness comes for free from the *target system's* immutability rules — e.g., cnpg's `bootstrap.*` field is immutable after Cluster creation, so a state flip after first materialization can't silently flip the bootstrap mode. Surfaced as `ChildApplied=False reason=ImmutableFieldRejected`.

A `BackupRepository` may have zero, one, or many BRCs pointing at it. Repos without any consumer still get continuous validation.

### Authoring variants with Kustomize

Variants are typically authored as standalone YAML files (with full IDE / CRD schema validation) and wrapped into ConfigMaps via Kustomize:

```yaml
# kustomization.yaml
configMapGenerator:
  - name: nextcloud-cnpg-cluster-recovery
    files:
      - cluster.yaml=cnpg-cluster-recovery.yaml
  - name: nextcloud-cnpg-cluster-init
    files:
      - cluster.yaml=cnpg-cluster-init.yaml
generatorOptions:
  disableNameSuffixHash: true            # stable name for the BRC reference
```

The BRC reads `data.cluster.yaml` from the rendered CM and applies it.

## Observability

Every reconcile cycle publishes:

| Metric | Type | Purpose |
|---|---|---|
| `arete_validation_runs_total{br,format,level,result}` | Counter | Success/failure rate per layer |
| `arete_validation_duration_seconds{br,format,level}` | Histogram | Job-latency trending |
| `arete_e4_throughput_bytes_per_sec{br,format}` | Gauge | RTO baseline |
| `arete_e4_bytes_transferred{br,format}` | Gauge | Last full-retrieval volume |
| `arete_backup_age_seconds{br,format}` | Gauge | Freshness alert input |
| `arete_inventory_objects{br,format}` | Gauge | Repo growth tracking |
| `arete_inventory_bytes{br,format}` | Gauge | Repo size tracking |
| `arete_condition_state{br,format,condition}` | Gauge | 1 / 0 / -1 per condition (universal alert input) |

The Helm chart includes:
- `ServiceMonitor` (gated by `prometheus.enable`)
- `PrometheusRule` with 8 alerts covering Health rollup, Reachable, BackupCurrent, per-layer failures, validation halt, throughput regression (gated by `prometheus.rules.enable`)

A Grafana dashboard JSON is shipped separately (see `dist/chart`); standard `grafana_dashboard: "1"` ConfigMap pickup pattern.

## Operator escape hatches

### `arete.io/force-revalidate`

Annotate a `BackupRepository` with an RFC3339 timestamp to bypass the per-level cooldown for a single cycle:

```bash
kubectl annotate br my-repo arete.io/force-revalidate=$(date -u +%Y-%m-%dT%H:%M:%SZ) --overwrite
```

The next reconcile spawns E2 (and E3 if enabled) immediately and records the value in `status.lastForceRevalidatedAt`, so the same annotation won't loop. Useful when the repository state changed in a way arete can't observe — e.g., a fresh prefix that just received its first backup, or after pre-existing validators failed against an empty pre-init prefix.

### `arete.io/force-e4`

Annotate a `BackupRepository` to trigger a one-shot E4 (full retrieval) Job, bypassing the `fullRetrievalInterval` schedule entirely:

```bash
kubectl annotate br my-repo arete.io/force-e4=$(date -u +%Y-%m-%dT%H:%M:%SZ) --overwrite
```

The spec must have `fullRetrievalStorageClass` and `fullRetrievalPVCSize` set (the annotation triggers the run but the controller still needs to know where and how big to provision the download PVC) — if either is missing, the spawn is refused loudly in the controller log and the annotation stays pending until the spec is fixed. `fullRetrievalInterval` does NOT need to be set; per the "E4 is on-demand only" design, per-tenant defaults should leave the interval unset and rely on this annotation for ad-hoc runs.

The value is recorded in `status.lastForcedE4At` once the resulting E4 Job completes, so the same annotation won't loop. Bump the timestamp to trigger a fresh run.

### Transient credentials tolerance

If the `credentialsSecret` referenced by a `BackupRepository` briefly disappears (typically during a tenant-namespace rebuild from a DR drill) the controller does *not* immediately flip the BR to unhealthy. It tolerates a 60-second gap, requeueing every 30s. Beyond that window, `Reachable` flips to `False` (with reason `CredentialsUnavailable`) but `MetadataValid` / `SampledIntegrityValid` / `FullRetrievalCompleted` are preserved at their last known-good values — a credentials gap is an *observability* problem, not evidence the repo data has rotted. Once credentials return and validation runs, conditions refresh through the normal path.

The `BackupRepositoryConditional` controller cooperates with this: when a BR's `Healthy` rollup is `Unknown` (vs definitively `False`) and the BRC has a prior successful decision, the BRC preserves that decision rather than flipping to `whenDegraded` — preventing the recovery path from being torn down by the very disturbance it's recovering from.

## Design rationale

Summary (full reasoning in the project's ADRs, particularly ADR-022 and ADR-023):

- **S3-only** — focused; constraint enables clean code
- **Two CRDs** — separates observation (`BackupRepository`) from downstream derivation (`BackupRepositoryConditional`)
- **Four E-levels of validation** — cheap monitoring (E1, E2) is always on; deep retrieval (E3, E4) is opt-in
- **Format validators are real binaries shipped by arete** — validation matches restoration; no silent drift between the validator's view and what restore would actually see
- **Always-latest validator** — doubles as a forward-compatibility canary for producer upgrades
- **Strict contract** — no escape hatches, no soft-fail tier
- **BRC variants reference manifests, not embed them** — keeps source files schema-validatable in IDEs; Kustomize bridge handles the wrap

## Install

Helm chart published as an OCI artifact:

```yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: arete
  namespace: flux-system
spec:
  interval: 5m
  url: oci://ghcr.io/tobiashofmaenner/charts/arete
  ref:
    tag: 0.5.17

---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: arete
  namespace: arete-system
spec:
  chartRef:
    kind: OCIRepository
    name: arete
    namespace: flux-system
  values:
    prometheus:
      enable: true
      rules:
        enable: true
    backupRepositoryConditional:
      childResourceTargets:                # GVKs the BRC controller may write
        - apiGroups: ["postgresql.cnpg.io"]
          resources: ["clusters"]
```

Add additional `childResourceTargets` if your BRCs materialize other GVKs.

## License

[Apache 2.0](LICENSE).
