"""
logistics-fusion-service bootstrap — thin wrapper around the shared library.

ADR-0023 Phase 6b §A: fusion subscribes to each of the 3 per-edge Kafka
clusters PLUS redpanda-hq for asset-cm-state (cm-service produces there).
The deployment registration is idempotent (409s are handled); each cluster
gets its own subscriptions with edge-suffixed consumer-group names.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time

from openddil_bootstrap.restate_subscriptions import (
    Subscription,
    bootstrap_restate_service,
    group_prefix_owner,
    prune_subscriptions,
)

logger = logging.getLogger("logistics.bootstrap")


RESTATE_ADMIN_URL    = os.getenv("RESTATE_ADMIN_URL",    "http://restate-server:9070")
FUSION_SERVICE_ENDPOINT = os.getenv("FUSION_SERVICE_ENDPOINT",
                                      "http://logistics-fusion-service:9081")
BOOTSTRAP_TIMEOUT_S  = int(os.getenv("FUSION_BOOTSTRAP_TIMEOUT_S", "120"))

DEFAULT_EDGE_CLUSTERS = (
    "edge-01=redpanda-edge-01:9092,"
    "edge-02=redpanda-edge-02:9092,"
    "edge-03=redpanda-edge-03:9092"
)
EDGE_CLUSTERS = os.getenv("FUSION_EDGE_CLUSTERS", DEFAULT_EDGE_CLUSTERS)
HQ_BROKERS = os.getenv("FUSION_HQ_BROKERS", "redpanda-hq:19092")


def _parse_clusters(spec: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for entry in spec.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if "=" not in entry:
            raise RuntimeError(
                f"FUSION_EDGE_CLUSTERS entry {entry!r} must be 'edge_id=host:port'"
            )
        edge_id, brokers = entry.split("=", 1)
        out.append((edge_id.strip(), brokers.strip()))
    return out


def _per_edge_subscriptions(edge_id: str) -> list[Subscription]:
    override = os.getenv("FUSION_SUBSCRIPTIONS")
    if override:
        try:
            entries = json.loads(override)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"FUSION_SUBSCRIPTIONS is not valid JSON: {exc}"
            ) from exc
        return [Subscription(**e) for e in entries]

    return [
        Subscription(
            topic="asset-telemetry-windows",
            handler="AssetLogistics/on_telemetry_window",
            consumer_group=f"fusion-service-windows-{edge_id}",
        ),
        Subscription(
            topic="raw-sensor-stream",
            handler="AssetLogistics/on_proprietary_update",
            consumer_group=f"fusion-service-silver-{edge_id}",
        ),
        Subscription(
            topic="derived-sustainment",
            handler="AssetLogistics/on_derived_sustainment",
            consumer_group=f"fusion-service-derived-{edge_id}",
        ),
        # Sub-phase F — engagement-worthiness. The weapons-capability
        # feed (source-specific messages decomposed by their respective
        # Bloblang) lands on asset-capability-snapshot (Silver, JSON).
        # The topic is created on every edge cluster
        # (topic-init parity), so subscribing on all edges is safe — the
        # consumer is simply idle on edges the customer feed does not
        # target (edge-01 by convention today).
        Subscription(
            topic="asset-capability-snapshot",
            handler="AssetLogistics/on_capability_snapshot",
            consumer_group=f"fusion-service-capability-{edge_id}",
        ),
    ]


def _hq_subscriptions() -> list[Subscription]:
    return [
        Subscription(
            topic="asset-cm-state",
            handler="AssetLogistics/on_cm_state_change",
            consumer_group="fusion-service-cm-state-hq",
        ),
    ]


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
    )

    edge_clusters = _parse_clusters(EDGE_CLUSTERS)
    # ADR-0023 Phase 6b §A — sequentialize cluster registrations to avoid
    # the Restate 1.6.2 subscription_controller race (see cm-service
    # bootstrap for the diagnosis). 2s default proven sufficient.
    inter_cluster_sleep = float(os.getenv("BOOTSTRAP_INTER_CLUSTER_SLEEP_S", "2"))
    logger.info("[fusion] bootstrapping %d edge cluster(s) + 1 HQ cluster "
                "(inter-cluster sleep=%.1fs)",
                len(edge_clusters), inter_cluster_sleep)

    desired: list[tuple[str, Subscription]] = []
    for i, (edge_id, brokers) in enumerate(edge_clusters):
        if i > 0 and inter_cluster_sleep > 0:
            time.sleep(inter_cluster_sleep)
        cluster_name = f"openddil-{edge_id}"
        bootstrap_restate_service(
            service_label=f"logistics-fusion-service[{edge_id}]",
            restate_admin_url=RESTATE_ADMIN_URL,
            service_endpoint=FUSION_SERVICE_ENDPOINT,
            kafka_cluster_name=cluster_name,
            kafka_brokers=brokers,
            subscriptions=_per_edge_subscriptions(edge_id),
            timeout_s=BOOTSTRAP_TIMEOUT_S,
        )
        desired.extend((cluster_name, sub)
                        for sub in _per_edge_subscriptions(edge_id))

    if inter_cluster_sleep > 0:
        time.sleep(inter_cluster_sleep)
    bootstrap_restate_service(
        service_label="logistics-fusion-service[hq]",
        restate_admin_url=RESTATE_ADMIN_URL,
        service_endpoint=FUSION_SERVICE_ENDPOINT,
        kafka_cluster_name="openddil-hq",
        kafka_brokers=HQ_BROKERS,
        subscriptions=_hq_subscriptions(),
        timeout_s=BOOTSTRAP_TIMEOUT_S,
    )
    # The HQ subscriptions belong in the desired set too -- they share the
    # `fusion-service-` ownership prefix, so omitting them here would have
    # the prune below delete the very subscription that carries a
    # tier-managed edge's CM state INTO the root. The completeness of this
    # list is what makes pruning safe, and `fusion-service-cm-state-hq` is
    # the entry whose absence would be quietest.
    desired.extend(("openddil-hq", sub) for sub in _hq_subscriptions())

    # ------------------------------------------------------------------
    # PRUNE (UD-12). See the companion comment in cm-service's bootstrap:
    # registration can only ADD, so a subscription retired from
    # FUSION_EDGE_CLUSTERS survived every upgrade except by way of
    # `hook-restate-wipe` deleting the PVC -- which is off in exactly the
    # prod-like configuration where it matters.
    # ------------------------------------------------------------------
    prune_subscriptions(
        restate_admin_url=RESTATE_ADMIN_URL,
        desired=desired,
        owns=group_prefix_owner("fusion-service-"),
        scope_label="logistics-fusion-service",
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
