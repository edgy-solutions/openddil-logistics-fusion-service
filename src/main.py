"""
openddil-logistics-fusion-service entrypoint.

Boots:
  1. Thresholds from env
  2. Ontology consistency check (WARN-level)
  3. Kafka producer (asset-logistics-status egress)
  4. Restate AssetLogistics Virtual Object on hypercorn ASGI

Environment:
  FUSION_KAFKA_BROKERS       Kafka bootstrap servers (default: redpanda-edge:9092)
  FUSION_HTTP_PORT           port the Restate endpoint listens on (default: 9081)
  ONTOLOGY_DIR               path to ontology YAMLs (default: /ontology)
  LOG_LEVEL                  INFO / DEBUG / WARN (default: INFO)
  + all Thresholds.from_env() fields (fuel/ammo/wear/mtbf bands, intervals)
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

from confluent_kafka import KafkaException, Producer

# Generated proto bindings are on PYTHONPATH (set by Dockerfile)
sys.path.insert(0, str(Path(__file__).parent))

from fusion.ontology_check import check_ontology_consistency
from fusion.thresholds import Thresholds
from workflows import asset_logistics

logger = logging.getLogger("logistics.main")


def _configure_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
    )


def _build_producer() -> Producer:
    brokers = os.getenv("FUSION_KAFKA_BROKERS", "redpanda-edge:9092")
    conf = {
        "bootstrap.servers":  brokers,
        "acks":               "all",
        "linger.ms":          20,
        "compression.type":   "zstd",
        "enable.idempotence": True,
    }
    producer = Producer(conf)
    try:
        producer.list_topics(timeout=5)
        logger.info("Kafka producer ready (brokers=%s)", brokers)
    except KafkaException as exc:
        logger.warning(
            "Kafka producer init returned warning (will retry on first send): %s",
            exc,
        )
    return producer


def _install_kafka_publisher(producer: Producer) -> None:
    def publish(topic: str, key: str, value: bytes) -> None:
        producer.produce(
            topic=topic,
            key=(key or "").encode("utf-8") if isinstance(key, str) else key,
            value=value,
        )
        producer.poll(0)
    asset_logistics.set_kafka_publisher(publish)
    logger.info("Kafka publisher installed on AssetLogistics handlers")


async def _run_server(producer: Producer) -> None:
    import restate
    from hypercorn.asyncio import serve
    from hypercorn.config import Config

    app = restate.app([asset_logistics.asset_logistics])

    cfg = Config()
    port = int(os.getenv("FUSION_HTTP_PORT", "9081"))
    cfg.bind = [f"0.0.0.0:{port}"]
    cfg.accesslog = "-"
    cfg.errorlog = "-"

    shutdown_event = asyncio.Event()

    def _on_signal(*_args):
        logger.info("Shutdown signal received; flushing producer...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            signal.signal(sig, lambda *_a: _on_signal())

    logger.info("Starting Restate AssetLogistics endpoint on :%d", port)
    server_task = asyncio.create_task(
        serve(app, cfg, shutdown_trigger=shutdown_event.wait),
    )
    try:
        await server_task
    finally:
        logger.info("Flushing Kafka producer (timeout=10 s)...")
        remaining = producer.flush(timeout=10)
        if remaining > 0:
            logger.warning(
                "%d producer message(s) not flushed before shutdown", remaining,
            )


def main() -> None:
    _configure_logging()

    # Self-check ontology consistency. WARN-level, non-fatal.
    check_ontology_consistency()

    # Install thresholds (env-driven, used by every handler invocation).
    thresholds = Thresholds.from_env()
    asset_logistics.set_thresholds(thresholds)
    logger.info(
        "Thresholds installed: fuel=%s/%s%%, ammo=%s/%s%%, wear=%s/%s%%, "
        "mtbf=%s/%sh, emit_interval=%ss, stale=%ss",
        thresholds.fuel_pct_critical, thresholds.fuel_pct_degraded,
        thresholds.ammo_pct_critical, thresholds.ammo_pct_degraded,
        thresholds.wear_pct_critical, thresholds.wear_pct_degraded,
        thresholds.mtbf_hours_critical, thresholds.mtbf_hours_degraded,
        thresholds.emit_interval_seconds, thresholds.stale_input_seconds,
    )

    producer = _build_producer()
    _install_kafka_publisher(producer)
    asyncio.run(_run_server(producer))


if __name__ == "__main__":
    main()
