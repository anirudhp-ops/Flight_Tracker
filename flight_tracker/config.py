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

from pydantic import Field, field_validator, model_validator
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

    # asyncpg pool sizing — see flight_tracker/db/OPTIMIZATION.md for the
    # rationale behind these specific numbers (min/max size, connection
    # recycling via max_queries, prepared-statement cache lifetime).
    db_pool_min_size: int = Field(default=10, gt=0)
    db_pool_max_size: int = Field(default=20, gt=0)
    db_pool_max_queries: int = Field(default=50000, gt=0)
    db_pool_max_cached_statement_lifetime: int = Field(default=300, ge=0)

    # --- Redis cache-aside layer (flight_tracker/cache/redis_cache.py) -----
    cache_flight_ttl_seconds: int = Field(default=300, gt=0)    # 5 min
    cache_airport_ttl_seconds: int = Field(default=600, gt=0)   # 10 min
    cache_delays_ttl_seconds: int = Field(default=120, gt=0)    # 2 min

    # How often the background job deletes long-landed rows from Postgres.
    db_cleanup_interval_seconds: int = Field(default=3600, gt=0)  # 1 hour
    db_cleanup_max_age_hours: float = Field(default=24, gt=0)

    # --- Kafka (flight_tracker/events/) -------------------------------------
    # The app runs on the host, not inside docker-compose, so it always
    # dials the host-mapped listener — "localhost:9092", whether that's the
    # native brew broker or docker-compose's kafka service (both publish on
    # 9092). KAFKA_BROKER_ADDRESS in .env.example is the in-network address
    # for when the app itself is containerized later; unused here until then.
    kafka_bootstrap_servers: str = "localhost:9092"

    kafka_topic_flight_events: str = "flight-events"
    kafka_topic_processed_flights: str = "processed-flights"
    kafka_topic_delay_predictions: str = "delay-predictions"
    kafka_topic_dead_letter: str = "dead-letter-events"

    kafka_consumer_group_processor: str = "flight-processor"
    kafka_consumer_group_predictor: str = "delay-predictor"
    kafka_consumer_group_websocket: str = "websocket-stream"

    kafka_consumer_lag_warning_threshold: int = Field(default=100, gt=0)
    kafka_dlq_warning_threshold: int = Field(default=10, gt=0)
    kafka_metrics_log_interval_seconds: int = Field(default=10, gt=0)

    @field_validator("enable_flightaware_api", mode="before")
    @classmethod
    def _parse_truthy_string(cls, v):
        """Accept the same truthy strings the old os.getenv(...).lower() check did."""
        if isinstance(v, str):
            return v.strip().lower() in {"1", "true", "yes", "on"}
        return v

    @model_validator(mode="after")
    def _pool_max_at_least_min(self):
        if self.db_pool_max_size < self.db_pool_min_size:
            raise ValueError(
                f"db_pool_max_size ({self.db_pool_max_size}) must be >= "
                f"db_pool_min_size ({self.db_pool_min_size})"
            )
        return self

    @property
    def live_api_enabled(self) -> bool:
        key = (self.flightaware_api_key or "").strip()
        return self.enable_flightaware_api and key != "" and key != "YOUR_API_KEY"


# Instantiated once at import time — this IS "load all env vars at startup".
settings = Settings()
