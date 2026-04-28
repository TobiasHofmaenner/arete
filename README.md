# arete

> Kubernetes operator for S3 backup repository validation.

Greek ἀρετή — *excellence at fulfilling one's purpose*. A backup's *arete* is its restorability.

---

## Status

Pre-alpha. Design locked, implementation in progress.

## What it does

For each `BackupRepository` you declare, arete:

- **Continuously probes S3** to verify the repository exists, is reachable, and is being written to (Layer 1, format-agnostic — works for any backup format pushing to S3)
- **Optionally invokes the format's validation commands** (e.g., `wal-g backup-list`, `restic check`, `barman check`) against the repository to verify backups are restorable (Layer 2, opt-in per repo)
- **Provides a unified inventory and health overview** of every backup repository in the cluster — one place to see what backups exist, whether they're healthy, and when each was last successfully written
- **Exposes status** as Kubernetes CR conditions and Prometheus metrics
- **Optionally derives sticky decisions** via `ConditionalConfig` resources that downstream deployments consume (e.g., "should this database bootstrap fresh or restore from backup?")

## Scope

In scope:
- S3 / S3-compatible backup repositories (MinIO, AWS S3, Ceph S3, etc.)
- Format-aware validation for `wal-g`, `restic`, `barman` (more as needed)
- Layer 1 monitoring and Prometheus metrics
- Sticky decisions for downstream consumers

Out of scope:
- Backup creation (delegated to existing tools)
- Backup restoration (delegated to existing tools)
- Retention enforcement (delegated to S3 lifecycle rules)
- DR drill orchestration (delegated to workflow engines)
- Non-S3 storage backends

## Two CRDs

```yaml
# Core: represents a backup repository
apiVersion: arete.io/v1
kind: BackupRepository
metadata:
  name: my-postgres-backups
spec:
  s3: { endpoint, bucket, prefix, credentialsRef }
  freshness: { maxAge: 25h }
  growth: { minDailyDelta: 1MiB }
  deepValidation:
    format: walg
    walg: { pgMajorVersion: 16, encryptionKeyRef: ... }
status:
  state: healthy
  observed: { backupsFound, latestBackup, integrityCheckPassed, ... }

---
# Optional: derive a sticky decision for a downstream consumer
apiVersion: arete.io/v1
kind: ConditionalConfig
metadata:
  name: my-bootstrap-decision
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

See [docs/design.md](docs/design.md) (forthcoming) for the architectural decision record. Summary:

- **S3-only**: focused; constraint enables clean code
- **Two CRDs**: separates observation from derivation
- **Two validation layers**: cheap monitoring for everything; deep validation opt-in
- **Real validation binaries bundled**: validation matches restoration; no silent drift between the validator's view and what restore would actually see
- **Sticky decisions**: controller carries lifecycle state that ephemeral pods can't

## License

[Apache 2.0](LICENSE).
