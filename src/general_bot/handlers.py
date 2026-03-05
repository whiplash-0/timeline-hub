from aiogram import Router
from aiogram.types import Message

from general_bot.services import Services

router = Router()


@router.message()
async def buffer_message(message: Message, services: Services) -> None:
    services.message_buffer.append(message, user=message.from_user)
