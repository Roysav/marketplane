import os

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, CliSettingsSource, PydanticBaseSettingsSource, SettingsConfigDict, YamlConfigSettingsSource


class MarketplaneConfig(BaseModel):
    address: str


class ControllerConfig(BaseModel):
    reconnect_backoff: float = Field(alias="reconnectBackoff")
    resync_interval: float = Field(alias="resyncInterval")


class PolymarketConfig(BaseModel):
    market_channel_url: str = Field(alias="marketChannelUrl")
    gamma_api_url: str = Field(alias="gammaApiUrl")
    cron_interval: float = Field(alias="cronInterval")
    page_size: int = Field(alias="pageSize")
    max_concurrency: int = Field(alias="maxConcurrency")
    max_assets: int = Field(alias="maxAssets")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="POLYMARKET_",
        env_nested_delimiter="__",
    )

    marketplane: MarketplaneConfig
    controller: ControllerConfig
    polymarket: PolymarketConfig

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        yaml_file = os.environ.get("POLYMARKET_CONFIG_FILE")
        if yaml_file is None:
            raise ValueError("POLYMARKET_CONFIG_FILE is not set")
        return (
            CliSettingsSource(settings_cls, cli_parse_args=True, cli_ignore_unknown_args=True),
            env_settings,
            YamlConfigSettingsSource(settings_cls, yaml_file=yaml_file),
        )
