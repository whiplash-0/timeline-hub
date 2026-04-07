from dataclasses import dataclass

from timeline_hub.infra.tasks import TaskScheduler
from timeline_hub.services.clip_store import ClipStore
from timeline_hub.services.message_buffer import ChatMessageBuffer


@dataclass(frozen=True, slots=True)
class Services:
    chat_message_buffer: ChatMessageBuffer
    task_scheduler: TaskScheduler
    clip_store: ClipStore
