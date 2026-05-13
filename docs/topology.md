# Logistics Fusion Service — Topology

## Inputs

```
                   ┌──────────────────────────┐
                   │   raw-sensor-stream      │  Silver, all feeds
                   │   (DIS + sim-a + prop.)  │
                   └──────────────┬───────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              ▼                   ▼                   ▼
      ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
      │  faust-edge  │    │  cm-service  │    │   logistics  │
      │  windowing   │    │  (AssetCM)   │    │  fusion svc  │
      │   agent      │    │              │    │ (AssetLogis- │
      └──────┬───────┘    └──────┬───────┘    │    tics)     │
             │                   │            └──────┬───────┘
             ▼                   ▼                   │
   ┌────────────────────┐  ┌──────────────┐         │
   │ asset-telemetry-   │  │ asset-cm-    │         │ filters DIS
   │   windows          │  │   state      │         │ (no sustainment)
   └─────────┬──────────┘  └──────┬───────┘         │
             │                    │                  │
             └─────────┬──────────┴──────────────────┘
                       │  on_telemetry_window
                       │  on_cm_state_change
                       │  on_proprietary_update
                       │  on_timer (cadenced)
                       ▼
           ┌─────────────────────────┐
           │ AssetLogistics V.O.     │ one instance per asset_id
           │ + pure-Python rules     │ durable state in Restate
           │ + Thresholds (env)      │
           │ + platform_reference    │ fuel capacity from ontology
           └──────────────┬──────────┘
                          │ emits when:
                          │   - is_initial=true (first observation)
                          │   - is_transition=true (severity changed)
                          │   - on_timer fires (cadenced, force_emit)
                          ▼
              ┌──────────────────────────┐
              │ asset-logistics-status   │ compacted, keyed by asset_id
              │ AssetLogisticsStatusUpd. │
              └──────────────┬───────────┘
                             │
                             ▼
              ┌──────────────────────────┐
              │  redpanda-connect-       │ AssetLogisticsStatusUpdate
              │   egress                 │  ↓ protobuf→json
              │  + system-b-egress.yaml  │  ↓ Bloblang vocabulary map
              │   (Bloblang)             │  ↓ format_json
              └──────────────┬───────────┘
                             │
                             ▼
              ┌──────────────────────────┐
              │ RabbitMQ:                │
              │  battle-mgmt.asset-status│  topic exchange
              │  routing key:            │  asset.<asset_id>.status
              │  asset.X.status          │  consumed by System B
              └──────────────────────────┘
```

## Why this shape

Three concerns intersect on per-asset logistics state:

1. **Streaming aggregation (Faust)** — windowed fuel/ammo/wear slopes over
   the last 15-60 minutes. Stateless across asset boundaries; idempotent
   under replay. Faust's table model fits.

2. **Per-asset durable workflow (Restate)** — the AssetLogistics Virtual
   Object owns the *latest known* telemetry/windows/cm-state per asset,
   the last-emitted severity, and a debounced timer. Restate's per-object
   durable state plus scheduled callbacks (`object_send(send_delay=...)`)
   replaces both an in-memory cache (Phase 2 anti-pattern) and a cron-like
   recheck scheduler. ADR-0014 placement test passes.

3. **Pure-Python rules** — `compute_logistics_status` takes Protobuf
   messages + Thresholds + a clock and returns a fully-populated
   AssetLogisticsStatus. No Faust import, no Restate import. Each
   evaluator (`_eval_fuel`, `_eval_ammo`, `_eval_wear`, `_eval_mtbf`,
   `_eval_subsystems`, `_eval_cm_state`, `_eval_staleness`) is
   independently unit-testable; combined coverage stays ≥95%.

## Ontology dependencies

- `platform_reference.yaml` — fuel capacity per platform_variant. Used by
  `_eval_fuel` to convert volume-unit fuel readings to %. Domain-curated;
  hot-reloadable; deployment override via `FUEL_CAPACITY_BY_VARIANT` env.
- `platform_variant_aliases.yaml` — referenced indirectly. The Silver
  events arriving here already have `asset.platform_variant` set to a
  canonical key (or "UNKNOWN") because the feed-side Bloblang did the
  alias lookup at ingestion. Drift across ontology files is caught by
  the WARN-level consistency check at fusion service startup.

## Lifecycle

```
  REGISTERED ──first telemetry──► ACTIVE ──no input for stale_input_s──► STALE
                                     │
                                     │ explicit decommission
                                     ▼
                              DECOMMISSIONED
```

(Today the AssetLogistics object does not model lifecycle explicitly —
the rules engine handles staleness as a ConstrainingFactor. Reserved for
a future iteration if we need lifecycle state separate from severity.)

## Restate state per asset (durable)

| Key | Type | Source | Purpose |
|---|---|---|---|
| `latest_telemetry_dict` | dict | Silver event (filtered to sustainment-carrying feeds) | input to rules |
| `latest_windows_dict` | dict | WindowedTelemetry | input to rules |
| `cm_state_dict` | dict | AsMaintainedConfiguration | input to rules |
| `last_emitted_severity` | int | computed | for is_transition detection |
| `status_revision` | uint64 | counter | monotonic per emission |
| `next_timer_ns` | int | computed | debounce on_timer scheduling |

**Important** (per Phase 3 bug #9): Restate state survives Kafka topic
purge. Test fixtures that want a clean asset must use
`restate state clear "AssetLogistics/<asset_id>"`, not just `rpk topic
delete asset-logistics-status`.
