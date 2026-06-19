from pathlib import Path
from typing import Any

import yaml
from pydantic_settings import BaseSettings, CliSettingsSource, PydanticBaseSettingsSource


def _read_yaml(path: Path) -> dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f) or {}


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


class _MappingSource(PydanticBaseSettingsSource):
    def __init__(self, settings_cls: type[BaseSettings], data: dict[str, Any]):
        super().__init__(settings_cls)
        self._data = data

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
        raise NotImplementedError

    def __call__(self) -> dict[str, Any]:
        return self._data


def layered_yaml_sources(
    settings_cls: type[BaseSettings],
    *,
    default_path: Path,
    override_path: Path,
    env_settings: PydanticBaseSettingsSource,
) -> tuple[PydanticBaseSettingsSource, ...]:
    data = _read_yaml(default_path)
    if override_path.is_file():
        data = deep_merge(data, _read_yaml(override_path))
    return (
        CliSettingsSource(settings_cls, cli_parse_args=True, cli_ignore_unknown_args=True),
        env_settings,
        _MappingSource(settings_cls, data),
    )
