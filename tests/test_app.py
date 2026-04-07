from unittest.mock import AsyncMock, patch

import pytest

from timeline_hub.app import _notify_superusers_and_stop_polling


@pytest.mark.asyncio
async def test_failure_shutdown_stops_polling_when_notifications_succeed() -> None:
    bot = AsyncMock()
    dispatcher = AsyncMock()

    with patch('timeline_hub.app.logger.exception'):
        await _notify_superusers_and_stop_polling(
            bot=bot,
            dispatcher=dispatcher,
            superuser_ids={1, 2},
        )

    assert bot.send_message.await_count == 2
    assert {call.kwargs['chat_id'] for call in bot.send_message.await_args_list} == {1, 2}
    dispatcher.stop_polling.assert_awaited_once()


@pytest.mark.asyncio
async def test_failure_shutdown_stops_polling_when_one_notification_fails() -> None:
    bot = AsyncMock()
    bot.send_message.side_effect = [RuntimeError('blocked'), None]
    dispatcher = AsyncMock()

    with patch('timeline_hub.app.logger.exception'):
        await _notify_superusers_and_stop_polling(
            bot=bot,
            dispatcher=dispatcher,
            superuser_ids={1, 2},
        )

    assert bot.send_message.await_count == 2
    assert {call.kwargs['chat_id'] for call in bot.send_message.await_args_list} == {1, 2}
    dispatcher.stop_polling.assert_awaited_once()


@pytest.mark.asyncio
async def test_failure_shutdown_stops_polling_when_all_notifications_fail() -> None:
    bot = AsyncMock()
    bot.send_message.side_effect = [RuntimeError('first'), RuntimeError('second')]
    dispatcher = AsyncMock()

    with patch('timeline_hub.app.logger.exception'):
        await _notify_superusers_and_stop_polling(
            bot=bot,
            dispatcher=dispatcher,
            superuser_ids={1, 2},
        )

    assert bot.send_message.await_count == 2
    assert {call.kwargs['chat_id'] for call in bot.send_message.await_args_list} == {1, 2}
    dispatcher.stop_polling.assert_awaited_once()
