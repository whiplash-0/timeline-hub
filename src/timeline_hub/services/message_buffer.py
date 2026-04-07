from aiogram.types import Message

from timeline_hub.types import ChatId

type Messages = list[Message]
type MessageGroup = tuple[Message, ...]
type MessageGroups = list[MessageGroup]


class ChatMessageBuffer:
    """Chat-scoped buffer for incoming Telegram messages.

    Messages are stored by `chat_id`. `peek()` is non-destructive, while
    `flush()` and `flush_grouped()` consume buffered messages for the chat.
    Grouping is computed in `message_id` order.

    Note:
        In Telegram private chats, `chat_id` is equal to the sender's
        `user_id`. Therefore either identifier may be used as the key
        when the bot operates exclusively in personal chats.
    """

    def __init__(self) -> None:
        self._messages: dict[ChatId, Messages] = {}
        self._versions: dict[ChatId, int] = {}

    def append(self, message: Message, *, chat_id: ChatId) -> None:
        self._messages.setdefault(chat_id, []).append(message)
        self._bump_version(chat_id)

    def peek(self, chat_id: ChatId) -> Messages:
        return list(self._messages.get(chat_id, []))

    def peek_grouped(self, chat_id: ChatId) -> MessageGroups:
        """Peek and group messages by contiguous `media_group_id`."""
        return self._group(self.peek(chat_id))

    def version(self, chat_id: ChatId) -> int:
        return self._versions.get(chat_id, 0)

    def flush(self, chat_id: ChatId) -> Messages:
        messages = self._messages.pop(chat_id, [])
        if messages:
            self._bump_version(chat_id)
        return messages

    def flush_grouped(self, chat_id: ChatId) -> MessageGroups:
        """Flush and group messages by contiguous `media_group_id`."""
        return self._group(self.flush(chat_id))

    def _bump_version(self, chat_id: ChatId) -> None:
        self._versions[chat_id] = self.version(chat_id) + 1

    @staticmethod
    def _group(messages: Messages) -> MessageGroups:
        groups: list[Messages] = []
        ordered_messages = sorted(messages, key=lambda m: m.message_id)

        for message in ordered_messages:
            if not groups:
                groups.append([message])
                continue
            if message.media_group_id is not None and message.media_group_id == groups[-1][-1].media_group_id:
                groups[-1].append(message)
            else:
                groups.append([message])

        return [tuple(group) for group in groups]
