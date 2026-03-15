from datetime import timedelta
from typing import Self

from pydantic_settings import BaseSettings, SettingsConfigDict

from general_bot.types import UserId

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
    # Telegram Bot API token used to authenticate the bot with Telegram
    bot_token: str

    # Telegram user IDs allowed to interact with the bot
    user_allowlist: list[UserId]

    # Delay used to batch forwarded messages before responding
    forward_batch_timeout: timedelta = timedelta(seconds=0.25)

    # Target loudness for normalized clips (LUFS)
    normalization_loudness: float = -14

    # Output bitrate for normalized clips (kbps)
    normalization_bitrate: int = 128

    model_config = _CONFIG

    @classmethod
    def load(cls, is_dev: bool) -> Self:
        bts = _BotTokenSettings()
        if is_dev and bts.bot_token_dev is None:
            raise ValueError('`BOT_TOKEN_DEV` is required in `.env` in dev mode')
        return cls(
            bot_token=bts.bot_token_dev if is_dev else bts.bot_token,
        )  # type: ignore[call-arg]  # pydantic-settings fills remaining fields from env at runtime; static checker false positive
