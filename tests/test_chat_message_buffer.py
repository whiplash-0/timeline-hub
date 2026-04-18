from aiogram.types import Message

from timeline_hub.services.message_buffer import ChatMessageBuffer


def _message(message_id: int, *, chat_id: int, media_group_id: str | None = None) -> Message:
    return Message.model_validate(
        {
            'message_id': message_id,
            'date': 1_700_000_000,
            'chat': {'id': chat_id, 'type': 'private'},
            'media_group_id': media_group_id,
        }
    )


def test_peek_and_flush_return_empty_list_for_unknown_chat() -> None:
    buffer = ChatMessageBuffer()

    assert buffer.peek(100) == []
    assert buffer.flush(100) == []
    assert buffer.version(100) == 0


def test_append_and_peek_returns_copy_without_mutating_internal_state() -> None:
    buffer = ChatMessageBuffer()
    first = _message(1, chat_id=100)
    second = _message(2, chat_id=100)

    buffer.append(first, chat_id=100)
    assert buffer.version(100) == 1
    buffer.append(second, chat_id=100)
    assert buffer.version(100) == 2

    peeked = buffer.peek(100)
    assert peeked == [first, second]
    assert buffer.version(100) == 2

    peeked.clear()
    assert buffer.peek(100) == [first, second]
    assert buffer.version(100) == 2


def test_peek_ordered_returns_messages_sorted_by_message_id() -> None:
    buffer = ChatMessageBuffer()
    third = _message(3, chat_id=100)
    first = _message(1, chat_id=100)
    second = _message(2, chat_id=100)

    buffer.append(third, chat_id=100)
    buffer.append(first, chat_id=100)
    buffer.append(second, chat_id=100)

    assert buffer.peek_flat(100) == [first, second, third]
    assert buffer.peek(100) == [third, first, second]
    assert buffer.version(100) == 3


def test_flush_returns_all_messages_for_chat_and_clears_it() -> None:
    buffer = ChatMessageBuffer()
    first = _message(1, chat_id=100)
    second = _message(2, chat_id=100)

    buffer.append(first, chat_id=100)
    buffer.append(second, chat_id=100)

    assert buffer.flush(100) == [first, second]
    assert buffer.version(100) == 3
    assert buffer.peek(100) == []
    assert buffer.flush(100) == []
    assert buffer.version(100) == 3


def test_chat_isolation_between_append_peek_and_flush() -> None:
    buffer = ChatMessageBuffer()
    a1 = _message(1, chat_id=100)
    a2 = _message(2, chat_id=100)
    b1 = _message(3, chat_id=200)

    buffer.append(a1, chat_id=100)
    buffer.append(b1, chat_id=200)
    buffer.append(a2, chat_id=100)

    assert buffer.peek(100) == [a1, a2]
    assert buffer.peek(200) == [b1]
    assert buffer.flush(100) == [a1, a2]
    assert buffer.peek(200) == [b1]


def test_flush_grouped_orders_by_message_id_and_groups_by_media_group_id() -> None:
    buffer = ChatMessageBuffer()
    # Intentionally append out of order to verify grouping uses message_id ordering.
    m4 = _message(4, chat_id=100, media_group_id='g2')
    m2 = _message(2, chat_id=100, media_group_id='g1')
    m5 = _message(5, chat_id=100, media_group_id='g2')
    m1 = _message(1, chat_id=100)
    m3 = _message(3, chat_id=100, media_group_id='g1')

    for message in [m4, m2, m5, m1, m3]:
        buffer.append(message, chat_id=100)

    assert buffer.flush_grouped(100) == [
        (m1,),
        (m2, m3),
        (m4, m5),
    ]
    assert buffer.version(100) == 6


def test_flush_grouped_uses_requested_chat_only() -> None:
    buffer = ChatMessageBuffer()
    chat_a_2 = _message(2, chat_id=100, media_group_id='a')
    chat_b_1 = _message(1, chat_id=200, media_group_id='b')
    chat_a_1 = _message(1, chat_id=100, media_group_id='a')
    chat_b_2 = _message(2, chat_id=200, media_group_id='b')

    buffer.append(chat_a_2, chat_id=100)
    buffer.append(chat_b_1, chat_id=200)
    buffer.append(chat_a_1, chat_id=100)
    buffer.append(chat_b_2, chat_id=200)

    assert buffer.flush_grouped(100) == [(chat_a_1, chat_a_2)]
    assert buffer.version(100) == 3
    assert buffer.peek(200) == [chat_b_1, chat_b_2]
    assert buffer.version(200) == 2
    assert buffer.flush_grouped(200) == [(chat_b_1, chat_b_2)]
    assert buffer.version(200) == 3
