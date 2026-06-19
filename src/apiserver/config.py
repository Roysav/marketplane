import os
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from utils.config import layered_yaml_sources


class PostgresConfig(BaseModel):
    connection_uri: str = Field(alias="connectionUri")


class RedisConfig(BaseModel):
    connection_uri: str = Field(alias="connectionUri")
    socket_timeout: float | None = Field(alias="socketTimeout")


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


class ServerConfig(BaseModel):
    address: str
    max_message_bytes: int = Field(alias="maxMessageBytes")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MARKETPLANE_APISERVER_",
        env_nested_delimiter="_",
    )

    logging: dict[str, Any]
    server: ServerConfig
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
        default_path = Path(__file__).parent / "default.config.yaml"
        override_path = Path(os.environ.get("MARKETPLANE_APISERVER_CONFIG_FILE", "apiserver.config.yaml"))
        return layered_yaml_sources(settings_cls, default_path=default_path, override_path=override_path, env_settings=env_settings)
