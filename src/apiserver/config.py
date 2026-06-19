import os
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, CliSettingsSource, PydanticBaseSettingsSource, SettingsConfigDict, YamlConfigSettingsSource


class PostgresConfig(BaseModel):
    connection_uri: str = Field(alias="connectionUri")


class RedisConfig(BaseModel):
    connection_uri: str = Field(alias="connectionUri")


class PostgresStorage(BaseModel):
    backend: Literal["postgres"]
    postgres: PostgresConfig


class RedisStorage(BaseModel):
    backend: Literal["redis"]
    redis: RedisConfig


class LedgerSettings(BaseModel):
    storage: PostgresStorage


class RecordsSettings(BaseModel):
    storage: PostgresStorage


class EventsSettings(BaseModel):
    storage: RedisStorage


class TicksSettings(BaseModel):
    storage: RedisStorage


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MARKETPLANE_",
        env_nested_delimiter="__",
    )

    logging: dict[str, Any]
    ledger: LedgerSettings
    records: RecordsSettings
    events: EventsSettings
    ticks: TicksSettings

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        yaml_file = os.environ.get("MARKETPLANE_APISERVER_CONFIG_FILE", str(Path(__file__).parent / "config.yaml"))
        return (
            CliSettingsSource(settings_cls, cli_parse_args=True, cli_ignore_unknown_args=True),
            env_settings,
            YamlConfigSettingsSource(settings_cls, yaml_file=yaml_file),
        )
