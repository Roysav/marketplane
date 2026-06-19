import sys

import pytest
from pydantic import ValidationError

from apiserver.config import Settings

_VALID = {
    "logging": {"version": 1, "root": {"level": "INFO"}},
    "ledger":  {"storage": {"backend": "postgres", "postgres": {"connectionUri": "postgresql://localhost/ledger"}}},
    "records": {"storage": {"backend": "postgres", "postgres": {"connectionUri": "postgresql://localhost/records"}}},
    "events":  {"storage": {"backend": "redis",    "redis":    {"connectionUri": "redis://localhost/0"}}},
    "ticks":   {"storage": {"backend": "redis",    "redis":    {"connectionUri": "redis://localhost/1"}}},
}


def _yaml(cfg: dict | None = None) -> str:
    import yaml
    return yaml.dump(cfg or _VALID)


@pytest.fixture
def cfg(tmp_path):
    f = tmp_path / "config.yaml"
    f.write_text(_yaml())
    return f


@pytest.fixture(autouse=True)
def isolate(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prog"])
    for key in list(__import__("os").environ):
        if key.startswith("MARKETPLANE_"):
            monkeypatch.delenv(key)


# --- yaml ---

def test_yaml_loads_all_fields(cfg, monkeypatch):
    monkeypatch.setenv("MARKETPLANE_APISERVER_CONFIG_FILE", str(cfg))
    s = Settings()
    assert s.ledger.storage.postgres.connection_uri == "postgresql://localhost/ledger"
    assert s.records.storage.postgres.connection_uri == "postgresql://localhost/records"
    assert s.events.storage.redis.connection_uri == "redis://localhost/0"
    assert s.ticks.storage.redis.connection_uri == "redis://localhost/1"


def test_yaml_missing_file_raises(monkeypatch):
    monkeypatch.setenv("MARKETPLANE_APISERVER_CONFIG_FILE", "nonexistent.yaml")
    with pytest.raises(ValidationError) as exc:
        Settings()
    locs = {e["loc"][0] for e in exc.value.errors()}
    assert locs == {"logging", "ledger", "records", "events", "ticks"}


def test_yaml_missing_field_raises(tmp_path, monkeypatch):
    import copy, yaml
    cfg = copy.deepcopy(_VALID)
    del cfg["ticks"]
    f = tmp_path / "config.yaml"
    f.write_text(yaml.dump(cfg))
    monkeypatch.setenv("MARKETPLANE_APISERVER_CONFIG_FILE", str(f))
    with pytest.raises(ValidationError) as exc:
        Settings()
    locs = [e["loc"][0] for e in exc.value.errors()]
    assert "ticks" in locs


def test_yaml_bad_discriminator_raises(tmp_path, monkeypatch):
    import copy, yaml
    cfg = copy.deepcopy(_VALID)
    cfg["ledger"]["storage"]["backend"] = "mysql"
    f = tmp_path / "config.yaml"
    f.write_text(yaml.dump(cfg))
    monkeypatch.setenv("MARKETPLANE_APISERVER_CONFIG_FILE", str(f))
    with pytest.raises(ValidationError) as exc:
        Settings()
    locs = [e["loc"] for e in exc.value.errors()]
    assert any(loc[0] == "ledger" for loc in locs)


# --- env ---

def test_env_overrides_yaml(cfg, monkeypatch):
    monkeypatch.setenv("MARKETPLANE_APISERVER_CONFIG_FILE", str(cfg))
    monkeypatch.setenv("MARKETPLANE_TICKS__STORAGE__REDIS__CONNECTIONURI", "redis://override:6379/9")
    s = Settings()
    assert s.ticks.storage.redis.connection_uri == "redis://override:6379/9"
    assert s.ledger.storage.postgres.connection_uri == "postgresql://localhost/ledger"


def test_yaml_does_not_override_env(cfg, monkeypatch):
    monkeypatch.setenv("MARKETPLANE_APISERVER_CONFIG_FILE", str(cfg))
    monkeypatch.setenv("MARKETPLANE_TICKS__STORAGE__REDIS__CONNECTIONURI", "redis://env:6379/9")
    s = Settings()
    assert s.ticks.storage.redis.connection_uri == "redis://env:6379/9"


def test_env_supplies_all_fields(tmp_path, monkeypatch):
    monkeypatch.setenv("MARKETPLANE_APISERVER_CONFIG_FILE", str(tmp_path / "nonexistent.yaml"))
    monkeypatch.setenv("MARKETPLANE_LOGGING", '{"version": 1}')
    monkeypatch.setenv("MARKETPLANE_LEDGER__STORAGE__BACKEND", "postgres")
    monkeypatch.setenv("MARKETPLANE_LEDGER__STORAGE__POSTGRES__CONNECTIONURI", "postgresql://env/ledger")
    monkeypatch.setenv("MARKETPLANE_RECORDS__STORAGE__BACKEND", "postgres")
    monkeypatch.setenv("MARKETPLANE_RECORDS__STORAGE__POSTGRES__CONNECTIONURI", "postgresql://env/records")
    monkeypatch.setenv("MARKETPLANE_EVENTS__STORAGE__BACKEND", "redis")
    monkeypatch.setenv("MARKETPLANE_EVENTS__STORAGE__REDIS__CONNECTIONURI", "redis://env/0")
    monkeypatch.setenv("MARKETPLANE_TICKS__STORAGE__BACKEND", "redis")
    monkeypatch.setenv("MARKETPLANE_TICKS__STORAGE__REDIS__CONNECTIONURI", "redis://env/1")
    s = Settings()
    assert s.ledger.storage.postgres.connection_uri == "postgresql://env/ledger"
    assert s.ticks.storage.redis.connection_uri == "redis://env/1"


def test_defaults_to_packaged_config(monkeypatch):
    s = Settings()
    assert s.ledger.storage.backend == "postgres"
    assert s.records.storage.backend == "postgres"
    assert s.events.storage.redis.connection_uri.startswith("redis://")
    assert s.ticks.storage.redis.connection_uri.startswith("redis://")


# --- cli ---

def test_cli_overrides_yaml_and_env(cfg, monkeypatch):
    monkeypatch.setenv("MARKETPLANE_APISERVER_CONFIG_FILE", str(cfg))
    monkeypatch.setenv("MARKETPLANE_TICKS__STORAGE__REDIS__CONNECTIONURI", "redis://env:6379/9")
    monkeypatch.setattr(sys, "argv", [
        "prog",
        "--ticks.storage.redis.connectionUri", "redis://cli:6379/7",
    ])
    s = Settings()
    assert s.ticks.storage.redis.connection_uri == "redis://cli:6379/7"


def test_cli_unknown_flags_ignored(cfg, monkeypatch):
    monkeypatch.setenv("MARKETPLANE_APISERVER_CONFIG_FILE", str(cfg))
    monkeypatch.setattr(sys, "argv", ["prog", "--workers", "4", "--host", "0.0.0.0"])
    s = Settings()
    assert s.ticks.storage.redis.connection_uri == "redis://localhost/1"


def test_cli_supplies_all_fields(tmp_path, monkeypatch):
    monkeypatch.setenv("MARKETPLANE_APISERVER_CONFIG_FILE", str(tmp_path / "nonexistent.yaml"))
    monkeypatch.setenv("MARKETPLANE_LOGGING", '{"version": 1}')
    monkeypatch.setattr(sys, "argv", [
        "prog",
        "--ledger.storage.backend", "postgres",
        "--ledger.storage.postgres.connectionUri", "postgresql://cli/ledger",
        "--records.storage.backend", "postgres",
        "--records.storage.postgres.connectionUri", "postgresql://cli/records",
        "--events.storage.backend", "redis",
        "--events.storage.redis.connectionUri", "redis://cli/0",
        "--ticks.storage.backend", "redis",
        "--ticks.storage.redis.connectionUri", "redis://cli/1",
    ])
    s = Settings()
    assert s.ledger.storage.postgres.connection_uri == "postgresql://cli/ledger"
    assert s.ticks.storage.redis.connection_uri == "redis://cli/1"
