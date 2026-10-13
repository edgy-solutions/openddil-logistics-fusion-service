"""Producer health: fatal-error exit and a delivery-based stall detector.

A Kafka idempotent producer can enter librdkafka's fatal state (for example
after a broker restart with batches in flight). From then on every produce()
raises, yet the process stays up and nothing restarts it. Two mechanisms end
that:

  * a fatal error exits the process (code 70) so the kubelet restarts it;
  * a stall detector, served on a liveness endpoint, catches a wedge that
    never surfaces as a fatal error.

WHY LIVENESS AND NOT READINESS
------------------------------
Readiness only removes a pod from Service endpoints. Nothing routes to this
producer, so a pod that is stuck stays stuck. The remedy for a wedge is a
restart, and restart is what liveness does.

WHY DELIVERY AND NOT produce()
------------------------------
produce() enqueues locally and succeeds while the client is disconnected, so
counting attempts would report progress exactly when there is none. Only a
confirmed delivery (the delivery callback with no error) is evidence of
output.

THE RULE
--------
    stalled == past the grace
               AND a produce was attempted within the window
               AND no confirmed delivery within the window

Both terms are required. An idle producer has nothing to deliver and is not
stalled; restarting a healthy pod every window would be worse than the wedge.
Thresholds are minutes by default for the same reason.
"""
from __future__ import annotations

import logging
import os
import threading
import time

from confluent_kafka import KafkaError

logger = logging.getLogger("logistics.producer_health")

FATAL_EXIT_CODE = 70

STALL_WINDOW_S = float(os.getenv("FUSION_PRODUCER_STALL_S", "180"))
STALL_GRACE_S = float(os.getenv("FUSION_PRODUCER_GRACE_S", "120"))

# Replaceable in tests.
_exit = os._exit
_now = time.monotonic

_lock = threading.Lock()
_started = _now()
_last_produce = 0.0
_last_delivered = 0.0


def is_fatal(err) -> bool:  # noqa: ANN001
    """True when the producer instance can never publish again.

    librdkafka reports fatal either through the error's own fatal() flag or
    by returning code _FATAL ("Local: Fatal error") from later calls, and the
    flag is not reliably set on those later errors, so test both.
    """
    if err is None:
        return False
    fatal = getattr(err, "fatal", None)
    if callable(fatal) and fatal():
        return True
    return err.code() == KafkaError._FATAL


def die_if_fatal(err, where: str) -> bool:  # noqa: ANN001
    """Exit the process when err is fatal. Returns False when it is not."""
    if not is_fatal(err):
        return False
    logger.critical(
        "FATAL Kafka producer state (%s, seen in %s): the producer cannot "
        "publish again. Exiting so the pod restarts.", err, where)
    # No flush: flushing a fatal producer blocks the exit.
    _exit(FATAL_EXIT_CODE)
    return True


def note_produce() -> None:
    """Called before every produce attempt: evidence of live input."""
    global _last_produce
    with _lock:
        _last_produce = _now()


def note_delivered() -> None:
    """Called only on a CONFIRMED delivery, never on produce()."""
    global _last_delivered
    with _lock:
        _last_delivered = _now()


def state() -> dict:
    now = _now()
    with _lock:
        lp, ld = _last_produce, _last_delivered
    up = now - _started
    p_age = (now - lp) if lp else None
    d_age = (now - ld) if ld else None

    if up < STALL_GRACE_S:
        return {"stalled": False, "reason": "grace", "uptime_s": round(up, 1),
                "window_s": STALL_WINDOW_S}

    produce_live = p_age is not None and p_age <= STALL_WINDOW_S
    delivery_live = d_age is not None and d_age <= STALL_WINDOW_S
    out = {
        "produce_age_s": None if p_age is None else round(p_age, 1),
        "delivery_age_s": None if d_age is None else round(d_age, 1),
        "window_s": STALL_WINDOW_S,
    }
    if produce_live and not delivery_live:
        return {"stalled": True, "reason": "produced, not delivered", **out}
    return {"stalled": False,
            "reason": "ok" if delivery_live else "idle", **out}
