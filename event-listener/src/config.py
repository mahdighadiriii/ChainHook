from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    web3_provider_url: str
    postgres_url: str
    redis_url: str
    rabbitmq_url: str

    class Config:
        env_file = "event-listener/.env"
        env_file_encoding = "utf-8"
        case_sensitive = False


settings = Settings()
