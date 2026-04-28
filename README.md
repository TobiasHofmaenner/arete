# arete

> Kubernetes operator for S3 backup repository validation.

Greek ἀρετή — *excellence at fulfilling one's purpose*. A backup's *arete* is its restorability.

---

## Status

Alpha. E1 (existence probe) shipped in v0.0.11 and runs in production against a real wal-g repository. Schema for the full E1–E4 vocabulary lands in v0.1.0 (breaking). E2/E3/E4 implementation in progress.

## What it does

For each `BackupRepository` you declare, arete validates the repository at four depth levels (E1–E4). Each level has its own cadence and its own status condition; higher levels are more thorough and more expensive.

| Level | What | Cost | Default |
|---|---|---|---|
| **E1** | Existence — bucket+prefix reachable, sentinel files present, recent backup within `maxBackupLag` | Trivial (S3 metadata only) | Always on (`probeInterval`, e.g. 5m) |
| **E2** | Metadata validation — the format's own validator accepts the metadata as well-formed (`wal-g backup-list` + `wal-g wal-verify`, `restic check`, `barman check`) | Low (small reads) | Always on (`metadataValidationInterval`, e.g. 6h) |
| **E3** | Sampled retrieval — random subset of objects fully downloaded and verified. **Bounded by fixed object count, not percent**, so cost stays scale-invariant | Bandwidth: bounded by `sampledRetrievalObjects` | Opt-in |
| **E4** | Full retrieval — every byte downloaded; written through a PVC at the customer's restore storage class then deleted per chunk; throughput measured. Doubles as RTO performance baseline | Bandwidth: 100% of repo | Opt-in only; manual trigger via annotation |

Beyond per-level validation, arete:

- **Provides a unified inventory and health overview** of every backup repository in the cluster — one place to see what backups exist, whether they're healthy, and when each was last successfully written
- **Exposes status** as Kubernetes CR conditions and Prometheus metrics with a `claimed*` / `observed*` / `verified*` provenance prefix structure (so consumers know what arete has actually proved versus what the producer self-reports)
- **Optionally derives sticky decisions** via `ConditionalConfig` resources that downstream deployments consume (e.g., "should this database bootstrap fresh or restore from backup?")

### Validator strategy: always latest, pinned per arete release

arete ships **one validator container image per format** (`arete-validator-walg`, `arete-validator-restic`, `arete-validator-barman`), pinned to the latest version of each tool per arete release. There is no per-version catalog; there is no per-tenant override. This is intentional — it makes arete double as a **forward-compatibility canary**: if a future arete release bumps the validator and validation starts failing against existing repos, you learn about the upcoming incompatibility *before* upgrading your production producer.

### Strict contract

If arete is running, your backups are in one of three states:
1. Safe.
2. arete is loudly screaming they aren't.
3. arete itself is not running (observable from outside).

There is no fourth state. No `ignoreErrors`, no `skipMetadataValidation`, no `pause`/`suspend`, no soft-fail tier on `Healthy`. If you don't want guarding, delete the CR.

## Scope

In scope:
- S3 / S3-compatible backup repositories (MinIO, AWS S3, Ceph RGW, R2, B2, etc.)
- Format-aware validation for `wal-g`, `restic`, `barman` (more as needed)
- E1–E4 monitoring with Prometheus metrics
- Sticky decisions for downstream consumers via `ConditionalConfig`

Out of scope:
- Backup creation (delegated to existing tools)
- Backup restoration (delegated to existing tools)
- Retention enforcement (delegated to S3 lifecycle rules)
- DR drill orchestration (delegated to workflow engines)
- Non-S3 storage backends

## Two CRDs

```yaml
# Core: represents a backup repository (cluster-scoped)
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
  format: walg                              # walg | restic | barman

  # Per-E intervals. Setting one is the enable signal for that level.
  probeInterval: 5m                         # E1 (required, 1m..1h)
  metadataValidationInterval: 6h            # E2 (required, 1h..24h)
  sampledRetrievalInterval: 24h             # E3 (optional)
  sampledRetrievalObjects: 50               # E3 (object count, scale-invariant)
  fullRetrievalInterval: ""                 # E4 (optional, off by default)

  # SLO knobs
  maxBackupLag: 25h                         # required, 1h..7d

status:
  conditions: [...]                         # Reachable, BackupCurrent, MetadataValid,
                                            # SampledIntegrityValid, FullRetrievalCompleted,
                                            # ProbeHealthy, ValidationHealthy, Healthy
  claimedLatestBackup: { name, createdAt, sizeBytes, ... }
  observedInventory: { objectCount, totalBytes, oldestObject, newestObject }
  verifiedLastValidationAt: ...

---
# Optional: derive a sticky decision for a downstream consumer
apiVersion: arete.arete.io/v1alpha1
kind: ConditionalConfig
metadata:
  name: my-bootstrap-decision
  namespace: customer
spec:
  repositoryRef: { name: my-postgres-backups }
  output:
    configMap:
      name: cluster-bootstrap-config
      keys:
        BOOTSTRAP_MODE:
          whenHealthy: "recovery"
          whenEmpty: "initdb"
        INCARNATION_ID:
          allocateOnce: "{date}-{shorthex}"
status:
  decided: recovery
  incarnationId: "..."
```

A `BackupRepository` may have zero, one, or many `ConditionalConfig`s pointing at it. Repos without any consumer still get continuous monitoring.

## Design rationale

Summary (full reasoning in the project's design ADRs):

- **S3-only**: focused; constraint enables clean code
- **Two CRDs**: separates observation from derivation
- **Four E-levels of validation**: cheap monitoring (E1) is always on; format validation (E2) is cheap and always on; deep retrieval (E3/E4) is opt-in
- **Format validators are real binaries shipped by arete**: validation matches restoration; no silent drift between the validator's view and what restore would actually see
- **Always-latest validator**: doubles as a forward-compatibility canary for producer upgrades
- **Strict contract**: no escape hatches, no soft-fail tier — the operator's job is to be the trustworthy contract
- **Sticky decisions**: controller carries lifecycle state that ephemeral pods can't

## License

[Apache 2.0](LICENSE).
