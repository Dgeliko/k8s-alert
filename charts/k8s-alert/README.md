# k8s-alert Helm chart

See [project README](../../README.md) for installation, profiles and release process.

| Value | Default | Purpose |
|---|---|---|
| `config.cluster` | `dev` | Cluster identifier in every alert |
| `config.env-type` | `dev` | Preset; `prod` alerts on first observed crash |
| `config.restarts.threshold` | inherited | dev=3/prod=1; explicit override |
| `config.restarts.window` | `5m` | Unique crash count window |
| `config.notifications.group-by` | `workload` | Workload or individual pod deduplication |
| `config.notifications.repeat-interval` | `30m` | Per-destination reminder interval |
| `config.logs.tail-lines` | `50` | Requested container log tail |
| `config.filters` | all | Namespace/pod name/kind include/exclude and labels |
| `destinations` | Pachca enabled | Optional Telegram and generic webhook |
| `reloader.enabled` | `true` | Integrate separately installed Stakater Reloader |
| `persistence.enabled` | `true` | Incident/outbox checkpoint in ConfigMap |
| `replicaCount` | `1` | Only supported value |

Disabled destinations do not require credentials or mounts. Secret values never
belong in values.yaml. Reloader watches only named configuration and active
credentials; state updates never trigger a reload. No chart dependencies.
