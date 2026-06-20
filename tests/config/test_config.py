import copy
import sys

import pytest
import yaml
from pydantic import ValidationError

from apiserver.config import Settings

_CONFIG = {
    "server":  {"address": "10.0.0.1:9000"},
    "ledger":  {"storage": {"postgres": {"connectionUri": "postgresql://localhost/ledger"}}},
    "records": {"storage": {"postgres": {"connectionUri": "postgresql://localhost/records"}}},
    "events":  {"storage": {"redis":    {"connectionUri": "redis://localhost/0"}}},
    "ticks":   {"storage": {"redis":    {"connectionUri": "redis://localhost/1"}}},
}


def _write(tmp_path, cfg, name="config.yaml"):
    f = tmp_path / name
    f.write_text(yaml.dump(cfg))
    return str(f)


@pytest.fixture(autouse=True)
def isolate(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prog"])
    for key in list(__import__("os").environ):
        if key.startswith("MARKETPLANE_"):
            monkeypatch.delenv(key)


@pytest.fixture
def config(tmp_path, monkeypatch):
    path = _write(tmp_path, _CONFIG)
    monkeypatch.setattr(sys, "argv", ["prog", "--config", path])
    return path


# --- layering: in-module defaults + --config files ---

def test_config_layers_over_in_module_defaults(config):
    s = Settings()
    assert s.server.address == "10.0.0.1:9000"
    assert str(s.ledger.storage.postgres.connection_uri) == "postgresql://localhost/ledger"
    assert str(s.ticks.storage.redis.connection_uri) == "redis://localhost:6379/1"
    assert s.server.max_message_bytes == 268435456
    assert s.ledger.storage.backend == "postgres"
    assert s.events.storage.redis.socket_timeout is None


def test_defaults_alone_are_incomplete(monkeypatch):
    with pytest.raises(ValidationError):
        Settings()


def test_missing_config_file_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prog", "--config", str(tmp_path / "absent.yaml")])
    with pytest.raises(FileNotFoundError):
        Settings()


def test_bad_discriminator_raises(tmp_path, monkeypatch):
    cfg = copy.deepcopy(_CONFIG)
    cfg["ledger"]["storage"]["backend"] = "mysql"
    monkeypatch.setattr(sys, "argv", ["prog", "--config", _write(tmp_path, cfg)])
    with pytest.raises(ValidationError):
        Settings()


def test_multiple_config_files_merge_in_order(tmp_path, monkeypatch):
    base = _write(tmp_path, _CONFIG, "base.yaml")
    override = _write(tmp_path, {"ticks": {"storage": {"redis": {"connectionUri": "redis://second/8"}}}}, "override.yaml")
    monkeypatch.setattr(sys, "argv", ["prog", "--config", base, "--config", override])
    s = Settings()
    assert str(s.ticks.storage.redis.connection_uri) == "redis://second:6379/8"
    assert str(s.ledger.storage.postgres.connection_uri) == "postgresql://localhost/ledger"


# --- env ---

def test_env_overrides_config(config, monkeypatch):
    monkeypatch.setenv("MARKETPLANE_APISERVER_TICKS_STORAGE_REDIS_CONNECTIONURI", "redis://env/9")
    s = Settings()
    assert str(s.ticks.storage.redis.connection_uri) == "redis://env:6379/9"
    assert str(s.ledger.storage.postgres.connection_uri) == "postgresql://localhost/ledger"


# --- cli field overrides ---

def test_cli_field_overrides_env_and_config(tmp_path, monkeypatch):
    path = _write(tmp_path, _CONFIG)
    monkeypatch.setenv("MARKETPLANE_APISERVER_TICKS_STORAGE_REDIS_CONNECTIONURI", "redis://env/9")
    monkeypatch.setattr(sys, "argv", ["prog", "--config", path, "--ticks.storage.redis.connectionUri", "redis://cli/7"])
    s = Settings()
    assert str(s.ticks.storage.redis.connection_uri) == "redis://cli:6379/7"


def test_cli_unknown_flags_ignored(config, monkeypatch):
    monkeypatch.setattr(sys, "argv", sys.argv + ["--workers", "4", "--host", "0.0.0.0"])
    s = Settings()
    assert str(s.ticks.storage.redis.connection_uri) == "redis://localhost:6379/1"
