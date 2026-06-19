import sys

import pytest
import yaml
from pydantic import ValidationError

from apiserver.config import Settings

_LOCAL = {
    "server":  {"address": "10.0.0.1:9000"},
    "ledger":  {"storage": {"postgres": {"connectionUri": "postgresql://localhost/ledger"}}},
    "records": {"storage": {"postgres": {"connectionUri": "postgresql://localhost/records"}}},
    "events":  {"storage": {"redis":    {"connectionUri": "redis://localhost/0"}}},
    "ticks":   {"storage": {"redis":    {"connectionUri": "redis://localhost/1"}}},
}


def _write(tmp_path, cfg):
    f = tmp_path / "apiserver.config.yaml"
    f.write_text(yaml.dump(cfg))
    return f


@pytest.fixture(autouse=True)
def isolate(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prog"])
    for key in list(__import__("os").environ):
        if key.startswith("MARKETPLANE_"):
            monkeypatch.delenv(key)


@pytest.fixture
def local(tmp_path, monkeypatch):
    f = _write(tmp_path, _LOCAL)
    monkeypatch.setenv("MARKETPLANE_APISERVER_CONFIG_FILE", str(f))
    return f


# --- layering: in-module defaults + project-root local override ---

def test_local_override_layers_over_in_module_defaults(local):
    s = Settings()
    assert s.server.address == "10.0.0.1:9000"
    assert s.ledger.storage.postgres.connection_uri == "postgresql://localhost/ledger"
    assert s.ticks.storage.redis.connection_uri == "redis://localhost/1"
    assert s.server.max_message_bytes == 268435456
    assert s.ledger.storage.backend == "postgres"
    assert s.events.storage.redis.socket_timeout is None


def test_committed_local_config_loads_by_default(monkeypatch):
    s = Settings()
    assert s.server.address.endswith(":50051")
    assert s.ledger.storage.backend == "postgres"
    assert s.ticks.storage.redis.connection_uri.startswith("redis://")


def test_missing_local_override_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("MARKETPLANE_APISERVER_CONFIG_FILE", str(tmp_path / "absent.yaml"))
    with pytest.raises(ValidationError):
        Settings()


def test_bad_discriminator_raises(tmp_path, monkeypatch):
    import copy
    cfg = copy.deepcopy(_LOCAL)
    cfg["ledger"]["storage"]["backend"] = "mysql"
    monkeypatch.setenv("MARKETPLANE_APISERVER_CONFIG_FILE", str(_write(tmp_path, cfg)))
    with pytest.raises(ValidationError):
        Settings()


# --- env ---

def test_env_overrides_local(local, monkeypatch):
    monkeypatch.setenv("MARKETPLANE_APISERVER_TICKS_STORAGE_REDIS_CONNECTIONURI", "redis://env/9")
    s = Settings()
    assert s.ticks.storage.redis.connection_uri == "redis://env/9"
    assert s.ledger.storage.postgres.connection_uri == "postgresql://localhost/ledger"


# --- cli ---

def test_cli_overrides_env_and_local(local, monkeypatch):
    monkeypatch.setenv("MARKETPLANE_APISERVER_TICKS_STORAGE_REDIS_CONNECTIONURI", "redis://env/9")
    monkeypatch.setattr(sys, "argv", ["prog", "--ticks.storage.redis.connectionUri", "redis://cli/7"])
    s = Settings()
    assert s.ticks.storage.redis.connection_uri == "redis://cli/7"


def test_cli_unknown_flags_ignored(local, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prog", "--workers", "4", "--host", "0.0.0.0"])
    s = Settings()
    assert s.ticks.storage.redis.connection_uri == "redis://localhost/1"
