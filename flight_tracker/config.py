"""
Single source of truth for environment configuration.

All environment variables are read here, exactly once, when this module is
first imported (which happens at process startup). Every other module
imports the `settings` singleton instead of calling os.getenv() directly, so
a given config value can't drift to a different default in two places.

An invalid or missing required value raises a pydantic ValidationError at
import time, naming the offending field, instead of failing later deep
inside the ingestion worker or a DB call.
"""
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Cost protection / ingestion source -------------------------------
    # Live FlightAware calls stay off unless BOTH of these are set, matching
    # the safe-by-default behavior described in README.md.
    enable_flightaware_api: bool = False
    flightaware_api_key: Optional[str] = None
    target_airport: str = "KJFK"
    poll_interval_seconds: int = Field(default=60, gt=0)

    # --- Redis --------------------------------------------------------------
    redis_host: str = "localhost"
    redis_port: int = Field(default=6379, gt=0, le=65535)

    # --- PostgreSQL -----------------------------------------------------------
    db_host: str = "localhost"
    db_port: int = Field(default=5432, gt=0, le=65535)
    db_name: str = "flight_tracker"
    # NOTE: must match the role you actually created locally (createuser
    # postgres / createdb flight_tracker) — "postgres" here, not a
    # developer's personal macOS username.
    db_user: str = "postgres"
    db_password: Optional[str] = None

    @field_validator("enable_flightaware_api", mode="before")
    @classmethod
    def _parse_truthy_string(cls, v):
        """Accept the same truthy strings the old os.getenv(...).lower() check did."""
        if isinstance(v, str):
            return v.strip().lower() in {"1", "true", "yes", "on"}
        return v

    @property
    def live_api_enabled(self) -> bool:
        key = (self.flightaware_api_key or "").strip()
        return self.enable_flightaware_api and key != "" and key != "YOUR_API_KEY"


# Instantiated once at import time — this IS "load all env vars at startup".
settings = Settings()
