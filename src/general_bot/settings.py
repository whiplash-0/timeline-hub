from datetime import timedelta
from typing import Self

from pydantic_settings import BaseSettings, SettingsConfigDict

_CONFIG = SettingsConfigDict(
    env_file='.env',
    frozen=True,
    extra='ignore',
)


class _BotTokenSettings(BaseSettings):
    bot_token: str
    bot_token_dev: str | None = None

    model_config = _CONFIG


class Settings(BaseSettings):
    bot_token: str
    allowlist: list[int]

    forward_batch_timeout: timedelta = timedelta(seconds=0.25)
    normalization_loudness: float = -14
    normalization_bitrate: int = 128

    model_config = _CONFIG

    @classmethod
    def load(cls, is_dev: bool) -> Self:
        bts = _BotTokenSettings()
        if is_dev and bts.bot_token_dev is None:
            raise ValueError('`BOT_TOKEN_DEV` is required in `.env` in dev mode')
        return cls(
            bot_token=bts.bot_token_dev if is_dev else bts.bot_token,
        )  # type: ignore[call-arg]
