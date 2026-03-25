from datetime import timedelta
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from general_bot.types import UserId


class S3Settings(BaseModel):
    endpoint_url: str
    region: str
    bucket: str
    access_key_id: str
    secret_access_key: SecretStr


class Settings(BaseModel):
    # Telegram bot
    bot_token: SecretStr
    superuser_ids: set[UserId]
    user_ids: set[UserId]

    # S3-compatible storage
    s3: S3Settings

    # Delay used to batch forwarded messages before responding
    forward_batch_timeout: timedelta = timedelta(seconds=0.25)

    # Padding line width in space units (1 unit ≈ width of one NBSP character)
    message_width: int = 80
    # Lowest year offered for clip store destinations
    min_clip_year: int = 2022

    # Audio normalization (LUFS target and bitrate)
    normalization_loudness: float = -14
    normalization_bitrate: int = 128

    model_config = ConfigDict(
        frozen=True,
    )

    @classmethod
    def load(cls, is_dev: bool) -> Self:
        env_settings = _EnvSettings()

        if env_settings.superuser_ids is None:
            raise ValueError('`SUPERUSER_IDS` is required in `.env`')
        if env_settings.s3 is None:
            raise ValueError('`S3__*` settings are required in `.env`')

        if is_dev:
            if env_settings.bot_token_dev is None:
                raise ValueError('`BOT_TOKEN_DEV` is required in `.env` in dev mode')
            bot_token = env_settings.bot_token_dev
        else:
            if env_settings.bot_token is None:
                raise ValueError('`BOT_TOKEN` is required in `.env`')
            bot_token = env_settings.bot_token

        return cls(
            bot_token=bot_token,
            superuser_ids=env_settings.superuser_ids,
            user_ids=env_settings.user_ids,
            s3=env_settings.s3,
        )

    @model_validator(mode='before')
    @classmethod
    def add_superusers_to_users(cls, data: Any) -> Any:
        if isinstance(data, dict) and ('user_ids' in data or 'superuser_ids' in data):
            data['user_ids'] = set(data.get('user_ids', [])) | set(data.get('superuser_ids', []))
        return data


class _EnvSettings(BaseSettings):
    bot_token: SecretStr | None = None
    bot_token_dev: SecretStr | None = None
    superuser_ids: set[UserId] | None = None
    user_ids: set[UserId] = Field(default_factory=set)
    s3: S3Settings | None = None

    model_config = SettingsConfigDict(
        env_file='.env',
        frozen=True,
        extra='ignore',
        env_nested_delimiter='__',
    )
