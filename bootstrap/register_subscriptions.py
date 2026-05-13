"""
logistics-fusion-service bootstrap — thin wrapper around the shared library.

Owns this service's subscription list; all the Restate-side plumbing
(deployment register, Kafka cluster register, dedup-and-create) is in
`openddil_bootstrap.restate_subscriptions`.
"""
from __future__ import annotations

import json
import logging
import os
import sys

from openddil_bootstrap.restate_subscriptions import (
    Subscription,
    bootstrap_restate_service,
)

logger = logging.getLogger("logistics.bootstrap")


# ---------------------------------------------------------------------------
# Configuration via env (no hardcoded topic names per ADR-0010)
# ---------------------------------------------------------------------------
RESTATE_ADMIN_URL    = os.getenv("RESTATE_ADMIN_URL",    "http://restate-server:9070")
FUSION_SERVICE_ENDPOINT = os.getenv("FUSION_SERVICE_ENDPOINT",
                                      "http://logistics-fusion-service:9081")
KAFKA_CLUSTER_NAME   = os.getenv("KAFKA_CLUSTER_NAME",   "openddil-edge")
KAFKA_BROKERS        = os.getenv("KAFKA_BROKERS",        "redpanda-edge:9092")
BOOTSTRAP_TIMEOUT_S  = int(os.getenv("FUSION_BOOTSTRAP_TIMEOUT_S", "120"))


def _subscriptions() -> list[Subscription]:
    """logistics-fusion-service's subscription list.

    Override via FUSION_SUBSCRIPTIONS (JSON list) to add feeds without code
    changes — same pattern as cm-service.
    """
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
        # Rolling windows from the Faust edge agent
        Subscription(
            topic=os.getenv("FUSION_TOPIC_WINDOWS", "asset-telemetry-windows"),
            handler="AssetLogistics/on_telemetry_window",
            consumer_group=os.getenv("FUSION_GROUP_WINDOWS",
                                       "fusion-service-windows"),
        ),
        # CM state changes from cm-service
        Subscription(
            topic=os.getenv("FUSION_TOPIC_CM_STATE", "asset-cm-state"),
            handler="AssetLogistics/on_cm_state_change",
            consumer_group=os.getenv("FUSION_GROUP_CM_STATE",
                                       "fusion-service-cm-state"),
        ),
        # Silver telemetry directly — for sustainment-carrying feeds (sim-a,
        # proprietary). The handler filters DIS-sourced events.
        Subscription(
            topic=os.getenv("FUSION_TOPIC_SILVER", "raw-sensor-stream"),
            handler="AssetLogistics/on_proprietary_update",
            consumer_group=os.getenv("FUSION_GROUP_SILVER",
                                       "fusion-service-silver"),
        ),
    ]


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
    )
    return bootstrap_restate_service(
        service_label="logistics-fusion-service",
        restate_admin_url=RESTATE_ADMIN_URL,
        service_endpoint=FUSION_SERVICE_ENDPOINT,
        kafka_cluster_name=KAFKA_CLUSTER_NAME,
        kafka_brokers=KAFKA_BROKERS,
        subscriptions=_subscriptions(),
        timeout_s=BOOTSTRAP_TIMEOUT_S,
    )


if __name__ == "__main__":
    sys.exit(main())
