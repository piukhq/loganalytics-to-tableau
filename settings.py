from typing import Optional

from pydantic import BaseSettings, PostgresDsn
from pydantic.networks import RedisDsn


class Settings(BaseSettings):
    pg_connection_string: PostgresDsn = "postgresql://postgres@localhost:5432/postgres"
    leader_election_enabled: bool = False
    redis_connection_string: Optional[RedisDsn]
    workspace_id: str = "eed2b98d-3396-4972-be3e-3e744532f7cd"


settings = Settings()
