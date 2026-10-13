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
  METRICS_PORT               HTTP port (default: 9464) serving Prometheus
                             /metrics and /healthz/live (503 when the producer
                             has attempted to publish but confirmed no
                             delivery within FUSION_PRODUCER_STALL_S; wire it
                             to the container's livenessProbe)
  FUSION_PRODUCER_STALL_S    window in seconds for the producer stall check
                             (default: 180)
  FUSION_PRODUCER_GRACE_S    seconds after start before the stall check may
                             report stalled (default: 120)
  + all Thresholds.from_env() fields (fuel/ammo/wear/mtbf bands, intervals)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

from confluent_kafka import KafkaException, Producer
from prometheus_client.exposition import MetricsHandler

# Generated proto bindings are on PYTHONPATH (set by Dockerfile)
sys.path.insert(0, str(Path(__file__).parent))

import producer_health
from fusion import effector_supply
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


class _HealthHandler(MetricsHandler):
    """/metrics (unchanged) plus /healthz/live on the same port."""

    def do_GET(self):  # noqa: N802
        if self.path.startswith("/healthz/live"):
            st = producer_health.state()
            body = json.dumps(st).encode()
            # 503 is what makes the kubelet restart the container.
            self.send_response(503 if st["stalled"] else 200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        super().do_GET()

    def log_message(self, *_a):  # noqa: ANN002
        pass  # probes arrive every few seconds; do not narrate them


def _start_metrics_server(port: int | None = None) -> ThreadingHTTPServer | None:
    """Expose the counters in `metrics.py` (e.g. the removal-unknown-key
    drop counter) for scraping, plus /healthz/live. Non-fatal: a port
    already in use (two instances on one host, a stale process) must not
    stop the service from doing its real job."""
    if port is None:
        port = int(os.environ.get("METRICS_PORT", "9464"))
    try:
        srv = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
    except OSError as exc:
        logger.warning(
            "Could not start metrics server on :%d (%s); "
            "continuing without metrics exposition", port, exc,
        )
        return None
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True,
                     name="metrics-health").start()
    logger.info("Metrics/health server listening on :%d", srv.server_address[1])
    return srv


def _error_cb(err) -> None:  # noqa: ANN001
    if producer_health.die_if_fatal(err, "error_cb"):
        return
    logger.warning("Kafka client error: %s", err)


def _on_delivery(err, _msg) -> None:  # noqa: ANN001
    if err is not None:
        if not producer_health.die_if_fatal(err, "delivery callback"):
            logger.warning("Kafka delivery failed: %s", err)
        return
    producer_health.note_delivered()


def _build_producer() -> Producer:
    brokers = os.getenv("FUSION_KAFKA_BROKERS", "redpanda-edge:9092")
    conf = {
        "bootstrap.servers":  brokers,
        "acks":               "all",
        "linger.ms":          20,
        "compression.type":   "lz4",   # NOT zstd: Restate's librdkafka has no
                                      # zstd, and one zstd batch kills its
                                      # consumer task forever (it restarts
                                      # at the same batch and dies again).
                                      # The chart's lz4 topic pin is a
                                      # broker-side mitigation of THIS line;
                                      # keep both, but this is the real fix.
        "enable.idempotence": True,
        "error_cb":           _error_cb,
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


_poll_stop = threading.Event()


def _poll_loop(producer: Producer) -> None:
    # Serves delivery callbacks between publishes. Without it a callback
    # waits for the next publish, and a quiet period after one produce would
    # read as "produced, not delivered" on a healthy pod.
    while not _poll_stop.is_set():
        try:
            producer.poll(0.5)
        except Exception as exc:  # noqa: BLE001
            logger.warning("producer poll failed: %s", exc)
            _poll_stop.wait(0.5)


def _install_kafka_publisher(producer: Producer) -> None:
    def publish(topic: str, key: str, value: bytes) -> None:
        producer_health.note_produce()
        try:
            producer.produce(
                topic=topic,
                key=(key or "").encode("utf-8") if isinstance(key, str) else key,
                value=value,
                on_delivery=_on_delivery,
            )
        except KafkaException as exc:
            if exc.args:
                producer_health.die_if_fatal(exc.args[0], "produce")
            raise
        producer.poll(0)
    asset_logistics.set_kafka_publisher(publish)
    _poll_stop.clear()
    threading.Thread(target=_poll_loop, args=(producer,), daemon=True,
                     name="producer-poll").start()
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
        _poll_stop.set()
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

    _start_metrics_server()

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

    # Declared-load table (same EFFECTOR_DECLARED_LOAD_PATH file the
    # projector reads). A malformed file is fatal at startup, same
    # discipline as the projector's loader for this file — a bad config
    # surfaces here, not later as a silently-wrong effector factor.
    try:
        declared_load_table = effector_supply.load_declared_load()
    except effector_supply.DeclaredLoadConfigError as exc:
        logger.error("Fatal: EFFECTOR_DECLARED_LOAD_PATH failed to validate: %s", exc)
        raise SystemExit(1) from exc
    asset_logistics.set_declared_load(declared_load_table)

    producer = _build_producer()
    _install_kafka_publisher(producer)
    asyncio.run(_run_server(producer))


if __name__ == "__main__":
    main()
