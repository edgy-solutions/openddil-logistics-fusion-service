# AGENTS.md — OpenDDIL Logistics Fusion Service

Guidelines and safety constraints for AI agents working in this repository.

## Repository Scope

This repo contains the **Logistics Fusion service** for OpenDDIL —
a Restate Virtual Object (`AssetLogistics`) that owns per-asset fused
logistics status. Consumes windowed telemetry + cm-state + Silver
sustainment; emits `AssetLogisticsStatusUpdate` to `asset-logistics-status`.

Phase 3.5 of the OpenDDIL build (see ADR-0014 for Restate-vs-Faust placement,
ADR-0016 for platform variant reconciliation).

## What You CAN Do

- **Add new evaluators** to `src/fusion/rules.py`. Each evaluator is a
  pure function: takes `FusionInputs` + `Thresholds` + `now_ns`, returns
  `ConstrainingFactor | None` (or `list[ConstrainingFactor]`).
- **Add new fields to `Thresholds`** in `src/fusion/thresholds.py`.
  Env-driven via `Thresholds.from_env()`; no separate YAML config schema.
- **Add new platform-intrinsic facts** to
  `openddil-contracts/ontology/platform_reference.yaml` (NOT here).
  Domain-curated reference data lives in the contracts repo.
- **Add new tests** to `src/tests/test_rules.py`. Coverage target on
  `rules.py` is ≥90% (currently 96%).
- **Extend the Restate workflow** in `src/workflows/asset_logistics.py`
  with new handlers / new state keys / new scheduled callbacks.

## What You MUST NOT Do

- ❌ **Never import Faust, Kafka, RabbitMQ, or Restate from `src/fusion/rules.py`**.
  ADR-0006 boundary: rules are pure Python. Streaming and durable-execution
  plumbing lives in `src/workflows/` and `src/main.py`.
- ❌ **Never bake customer-specific shape knowledge into the fusion engine**.
  The fusion service consumes generic Silver `EntityTelemetryEvent` +
  `WindowedTelemetry` + `AsMaintainedConfiguration`. Per-feed shape lives
  in Bloblang mappings (in `openddil-customer-bundle/dynamic-mappings/`).
- ❌ **Never bypass the shared bootstrap library**. The
  `bootstrap/register_subscriptions.py` wrapper MUST call
  `openddil_bootstrap.restate_subscriptions.bootstrap_restate_service`.
- ❌ **Never fabricate fuel% from absolute fuel volumes without a known
  platform capacity**. `_eval_fuel` returns None when the variant isn't
  in `platform_reference.yaml` — silence is correct, fabrication is not.
- ❌ **Never assume `clear_asset_logistics_state` succeeded**. Restate
  state survives Kafka topic purges; sometimes lingers across CLI clears.
  Tests should tolerate this rather than depend on a clean slate.

## Vocabulary Discipline

Internal severity vocabulary is `LogisticsSeverity_{OK,DEGRADED,CRITICAL,
NON_OPERATIONAL}` (the proto enum). This is the STABLE, TESTED vocabulary.
Customer-facing labels (FMC/PMC/NMC, green/amber/red, etc.) are applied
at the EGRESS boundary via Bloblang `match` blocks — NEVER inside the
fusion service.

If a customer demands a new internal severity level, that's a proto
change in `openddil-contracts/proto/openddil/logistics/v1/logistics_status.proto`
and a fusion-rules change here, not a vocabulary swap.

## Ontology Drift Check

`src/fusion/ontology_check.py` runs at service startup and logs WARN
for any `platform_variant` referenced by an alias file
(`platform_variant_aliases.yaml`, `dis_entity_types.yaml`) that lacks
an entry in `platform_reference.yaml`. Catches drift at boot rather
than at first wrong status emission.

If you add a new evaluator that depends on a new ontology field, extend
this check to flag missing entries for the new field too.

## Docker Compose Conventions (cross-repo rule)

When this service is consumed by `openddil-demo/docker-compose.yml`:

- The base compose references `image: ghcr.io/edgy-solutions/openddil/logistics-fusion-service:latest`.
  Both the main service and its bootstrap container use the same image
  with different entrypoints.
- `openddil-demo/docker-compose.override.yml` has the matching
  `build: { context: ../openddil-logistics-fusion-service }` and source
  mounts for developer hot-reload.
- **When you change the Dockerfile or pyproject.toml here**, publish a
  new image to `ghcr.io/edgy-solutions/openddil/logistics-fusion-service:latest` (or
  bump a tag) so the base compose works for non-developer consumers.

## Tests

`pytest src/tests/test_rules.py --cov=fusion --cov-report=term-missing`
should report:
- 46+ tests passing
- rules.py coverage ≥90%

`src/tests/conftest.py` sets `ONTOLOGY_DIR` to point at the in-tree
contracts repo's ontology dir so tests find `platform_reference.yaml`
without needing the container `/ontology` mount.

## Running from source — respect the dependency pins

Prefer running this service **from its image**. If you must run it from
source, install against the pins in `pyproject.toml`, not by name.

The one that bites: `cloudevents` is pinned `>=1.10.0,<2.0.0`. A bare
`pip install cloudevents` resolves to **2.x**, which does **not** provide
`cloudevents.conversion` — the import fails at startup with
`ModuleNotFoundError: No module named 'cloudevents.conversion'`, which
reads like a missing package rather than a wrong major version. Cost an
hour of harness debugging once; costs nothing to avoid.

Same caution applies to the sibling Restate service (`cm-service`), which
carries the same pin.

## Documentation Maintenance

After ANY structural change, update:
1. `docs/topology.md` — data-flow diagram + state schema.
2. `llms.txt` — high-level summary.
3. `.cursorrules` — only if new conventions are introduced.
4. This file — only if new safety constraints apply.
