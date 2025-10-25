import os

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    web3_provider_url: str | None = None
    postgres_url: str
    redis_url: str
    rabbitmq_url: str
    bitcoin_api_url: str | None = None
    solana_ws_url: str | None = None

    class Config:
        env_file = os.path.join(os.path.dirname(__file__), "..", ".env")
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "allow"


settings = Settings()
