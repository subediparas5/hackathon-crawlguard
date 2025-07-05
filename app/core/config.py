from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql://postgres:postgres@localhost:5432/crawlguard"

    # Application
    environment: str = "development"
    debug: bool = True
    secret_key: str = "your-secret-key-here-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30

    # Server
    host: str = "0.0.0.0"
    port: int = 8000

    # Logging
    log_level: str = "INFO"

    # AI/ML Services
    deepseek_api_key: str = "sk-5e52629cf4124e68a29c57987cbb1e8a"

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
