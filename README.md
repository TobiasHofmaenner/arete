# arete

> Kubernetes operator for S3 backup repository validation.

**Pronounced** /uh-REH-tay/. Greek ἀρετή — *excellence at fulfilling one's purpose*. A backup's *arete* is its restorability.

---

## Status

Pre-alpha. Design locked, implementation in progress.

## What it does

For each `BackupRepository` you declare, arete:

- **Continuously probes S3** to verify the repository exists, is reachable, and is being written to (Layer 1, format-agnostic — works for any backup format pushing to S3)
- **Optionally runs the real backup tool** against the repository (`wal-g`, `restic`, `barman`) to validate restorability (Layer 2, opt-in per repo)
- **Exposes unified health status** as Kubernetes CR conditions and Prometheus metrics
- **Optionally derives sticky decisions** via `ConditionalConfig` resources that downstream deployments consume (e.g., "should this Postgres cluster bootstrap fresh or restore from backup?")

## Why

Backup-and-restore architectures often fail silently — backups complete, files exist in S3, but actual restoration breaks at the worst possible moment because validation tested a *proxy* (custom S3 layout parsing, status delegation, metadata-only checks) rather than what restore would actually see.

arete validates using the **same binaries that would perform the restore**. A passing validation is a near-dry-run of the restoration path. If `wal-g backup-list` says "I see 12 valid backups, latest is X," then a restore against the same repository at the same moment will see those backups too.

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
- DR drill orchestration (delegated to workflow engines like Argo Workflows)
- Non-S3 storage backends

## Two CRDs

```yaml
# Core: represents a backup repository
apiVersion: arete.io/v1
kind: BackupRepository
metadata:
  name: athenesa-postgres
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
  name: athenesa-bootstrap
spec:
  repositoryRef: { name: athenesa-postgres }
  output:
    configMap:
      name: nextcloud-cnpg-bootstrap
      keys:
        BOOTSTRAP_MODE:
          whenHealthy: "recovery"
          whenEmpty: "initdb"
        INCARNATION_ID:
          allocateOnce: "{date}-{shorthex}"
status:
  decided: recovery
  incarnationId: "2026-04-28-a4f1c3"
```

A `BackupRepository` may have zero, one, or many `ConditionalConfig`s pointing at it. Repos without any consumer still get continuous monitoring.

## Design rationale

See [docs/design.md](docs/design.md) (forthcoming) for the architectural decision record. Summary:

- **S3-only**: focused; constraint enables clean code
- **Two CRDs**: separates observation from derivation
- **Two validation layers**: cheap monitoring for everything; deep validation opt-in
- **Real binaries bundled**: validation matches restoration; no silent drift
- **Sticky decisions**: controller carries lifecycle state that ephemeral pods can't

## License

[Apache 2.0](LICENSE).
