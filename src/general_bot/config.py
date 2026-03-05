from datetime import timedelta

from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    bot_token: str
    allowlist: list[int]
    forward_batch_timeout: timedelta = timedelta(seconds=0.5)

    model_config = SettingsConfigDict(
        env_file='.env',
        frozen=True,
    )


config = Config()  # type: ignore[call-arg]
