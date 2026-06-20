from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from utils.config import layered_yaml_sources


class MarketplaneConfig(BaseModel):
    address: str
    max_message_bytes: int = Field(alias="maxMessageBytes")


class ControllerConfig(BaseModel):
    reconnect_backoff: float = Field(alias="reconnectBackoff")
    resync_interval: float = Field(alias="resyncInterval")


class SignerConfig(BaseModel):
    private_key: str = Field(alias="privateKey")
    signature_type: str = Field(alias="signatureType")
    funder: str


class PolymarketConfig(BaseModel):
    market_channel_url: str = Field(alias="marketChannelUrl")
    user_channel_url: str = Field(alias="userChannelUrl")
    gamma_api_url: str = Field(alias="gammaApiUrl")
    clob_api_url: str = Field(alias="clobApiUrl")
    chain_id: int = Field(alias="chainId")
    tradespace: str
    order_lease: float = Field(alias="orderLease")
    cron_interval: float = Field(alias="cronInterval")
    page_size: int = Field(alias="pageSize")
    max_concurrency: int = Field(alias="maxConcurrency")
    max_assets: int = Field(alias="maxAssets")
    ping_timeout: float = Field(alias="pingTimeout")
    ping_interval: float = Field(alias="pingInterval")
    signer: SignerConfig


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MARKETPLANE_POLYMARKET_CONTROLLER_",
        env_nested_delimiter="_",
    )

    logging: dict[str, Any]
    marketplane: MarketplaneConfig
    controller: ControllerConfig
    polymarket: PolymarketConfig

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        env_settings: PydanticBaseSettingsSource,
        **kwargs: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        default_path = Path(__file__).parent / "default.config.yaml"
        return layered_yaml_sources(settings_cls, default_path=default_path, env_settings=env_settings)
