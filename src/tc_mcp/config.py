from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Connector(BaseModel):
    name: str
    base_url: str
    protocol_url: str | None = None
    email: str | None = None
    password: SecretStr | None = None
    token: SecretStr | None = None


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TC_", env_file=".env", extra="ignore")

    connectors: list[Connector] = Field(min_length=1)
    timeout_s: float = 30.0
    read_only: bool = False
    max_concurrency: int = Field(default=4, ge=1)
