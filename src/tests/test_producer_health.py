"""Producer fatal-exit and delivery-stall behaviour."""
from __future__ import annotations

import json
import urllib.error
import urllib.request

import pytest
from confluent_kafka import KafkaError, KafkaException

# conftest.py puts src/ and the generated bindings on sys.path.
import main as fx_main  # noqa: E402
import producer_health as ph  # noqa: E402
from workflows import asset_logistics  # noqa: E402


class FakeErr:
    def __init__(self, fatal: bool = False, code: int = 0):
        self._fatal, self._code = fatal, code

    def fatal(self):
        return self._fatal

    def code(self):
        return self._code

    def __str__(self):
        return f"FakeErr(fatal={self._fatal}, code={self._code})"


FATAL = FakeErr(fatal=True)
NONFATAL = FakeErr(fatal=False, code=-195)


@pytest.fixture
def exits(monkeypatch):
    calls: list[int] = []
    monkeypatch.setattr(ph, "_exit", calls.append)
    return calls


@pytest.fixture
def clock(monkeypatch):
    """Controllable monotonic clock with a clean stall state."""
    t = {"now": 10_000.0}
    monkeypatch.setattr(ph, "_now", lambda: t["now"])
    monkeypatch.setattr(ph, "_started", 10_000.0)
    monkeypatch.setattr(ph, "_last_produce", 0.0)
    monkeypatch.setattr(ph, "_last_delivered", 0.0)
    monkeypatch.setattr(ph, "STALL_WINDOW_S", 180.0)
    monkeypatch.setattr(ph, "STALL_GRACE_S", 120.0)
    return t


# -- is_fatal / die_if_fatal -------------------------------------------------

def test_is_fatal_flag():
    assert ph.is_fatal(FakeErr(fatal=True)) is True


def test_is_fatal_code_when_flag_unset():
    assert ph.is_fatal(FakeErr(fatal=False, code=KafkaError._FATAL)) is True


def test_is_fatal_nonfatal():
    assert ph.is_fatal(NONFATAL) is False


def test_is_fatal_none():
    assert ph.is_fatal(None) is False


def test_die_if_fatal_exits_70_once(exits):
    assert ph.die_if_fatal(FATAL, "test") is True
    assert exits == [70]


def test_die_if_fatal_nonfatal_does_not_exit(exits):
    assert ph.die_if_fatal(NONFATAL, "test") is False
    assert exits == []


# -- stall state -------------------------------------------------------------

def test_inside_grace_not_stalled(clock):
    clock["now"] += 60
    ph.note_produce()
    clock["now"] += 30  # still inside the 120 s grace
    st = ph.state()
    assert st["stalled"] is False and st["reason"] == "grace"


def test_idle_not_stalled(clock):
    clock["now"] += 1000  # past grace, nothing ever produced
    assert ph.state()["stalled"] is False
    ph.note_produce()
    clock["now"] += 500  # produce is now outside the window
    st = ph.state()
    assert st["stalled"] is False and st["reason"] == "idle"


def test_produced_not_delivered_is_stalled(clock):
    clock["now"] += 1000
    ph.note_produce()
    clock["now"] += 10
    st = ph.state()
    assert st["stalled"] is True


def test_produced_and_delivered_not_stalled(clock):
    clock["now"] += 1000
    ph.note_produce()
    clock["now"] += 5
    ph.note_delivered()
    clock["now"] += 5
    st = ph.state()
    assert st["stalled"] is False and st["reason"] == "ok"


# -- main wiring -------------------------------------------------------------

class FakeProducer:
    last_conf: dict = {}

    def __init__(self, conf):
        FakeProducer.last_conf = conf
        self.produce_exc: Exception | None = None
        self.produce_kwargs: dict = {}

    def list_topics(self, timeout=0):
        return None

    def produce(self, **kw):
        self.produce_kwargs = kw
        if self.produce_exc is not None:
            raise self.produce_exc

    def poll(self, _t=0):
        return 0


@pytest.fixture
def publisher(monkeypatch):
    """Install the real publish closure over a fake producer."""
    captured = {}
    monkeypatch.setattr(asset_logistics, "set_kafka_publisher",
                        lambda fn: captured.update(fn=fn))
    fake = FakeProducer({})
    fx_main._install_kafka_publisher(fake)
    yield fake, captured["fn"]
    fx_main._poll_stop.set()


def test_build_producer_error_cb_exits_on_fatal(monkeypatch, exits):
    monkeypatch.setattr(fx_main, "Producer", FakeProducer)
    fx_main._build_producer()
    cb = FakeProducer.last_conf["error_cb"]
    cb(NONFATAL)
    assert exits == []
    cb(FATAL)
    assert exits == [70]


def test_publish_fatal_produce_exits(publisher, exits):
    fake, publish = publisher
    fake.produce_exc = KafkaException(FATAL)
    with pytest.raises(KafkaException):
        publish("t", "k", b"v")
    assert exits == [70]


def test_publish_nonfatal_kafka_exception_reraises_no_exit(publisher, exits):
    fake, publish = publisher
    fake.produce_exc = KafkaException(NONFATAL)
    with pytest.raises(KafkaException):
        publish("t", "k", b"v")
    assert exits == []


def test_publish_buffer_error_propagates_no_exit(publisher, exits):
    fake, publish = publisher
    fake.produce_exc = BufferError("queue full")
    with pytest.raises(BufferError):
        publish("t", "k", b"v")
    assert exits == []


def test_publish_notes_produce_and_passes_callback(publisher, clock):
    fake, publish = publisher
    publish("t", "k", b"v")
    assert fake.produce_kwargs["on_delivery"] is fx_main._on_delivery
    assert ph._last_produce == clock["now"]


def test_delivery_callback_success_advances_delivered(clock):
    clock["now"] += 50
    fx_main._on_delivery(None, object())
    assert ph._last_delivered == clock["now"]


def test_delivery_callback_error_does_not_advance(clock, exits):
    fx_main._on_delivery(NONFATAL, object())
    assert ph._last_delivered == 0.0 and exits == []
    fx_main._on_delivery(FATAL, object())
    assert exits == [70]


# -- health HTTP -------------------------------------------------------------

def _get(port: int, path: str):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}",
                                    timeout=5) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


@pytest.fixture
def server():
    srv = fx_main._start_metrics_server(0)
    assert srv is not None
    yield srv.server_address[1]
    srv.shutdown()
    srv.server_close()


def test_healthz_live_200_when_not_stalled(server, clock):
    clock["now"] += 1000
    ph.note_produce()
    ph.note_delivered()
    status, body = _get(server, "/healthz/live")
    assert status == 200
    assert json.loads(body)["stalled"] is False


def test_healthz_live_503_when_stalled(server, clock):
    clock["now"] += 1000
    ph.note_produce()  # produced, never delivered
    status, body = _get(server, "/healthz/live")
    assert status == 503
    assert json.loads(body)["stalled"] is True


def test_metrics_200(server):
    status, _ = _get(server, "/metrics")
    assert status == 200
