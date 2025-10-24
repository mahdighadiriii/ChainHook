from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    postgres_url: str
    redis_url: str
    rabbitmq_url: str
    webhook_secret_key: str = "change-in-production"
    max_retry_attempts: int = 5
    retry_backoff_base: int = 2
    webhook_timeout: int = 30

    class Config:
        env_file = "webhook-orchestrator/.env"
        env_file_encoding = "utf-8"
        case_sensitive = False


settings = Settings()
