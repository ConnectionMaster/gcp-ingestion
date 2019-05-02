"""Classes for creating message batches."""

from .types import BatchSettings, ListBatchCallback, PubsubMessage
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional
import asyncio


@dataclass
class ListBatch:
    batch_settings: BatchSettings
    callback: Callable[[List[Message]], Awaitable]
    size = field(default=0, init=False)
    byte_size = field(default=0, init=False)
    full = field(default_factory=asyncio.Event, init=False)

    # extract batch settings
    def __post_init__(self):
        self.max_bytes = batch_settings.max_bytes
        self.max_latency = batch_settings.max_latency
        self.max_messages = batch_settings.max_messages
        if self.max_bytes is None:
            self.max_bytes = inf
        if self.max_latency is None:
            self.max_latency = inf
        if self.max_messages is None:
            self.max_messages = inf

    def add(self, message: PubsubMessage):
        try:
            self.result.append(message)
        except AttributeError:
            if self.batch_settings.strict_max_bytes and self.byte_size > self.max_bytes:
                raise ValueError("Single message exceeds MaxBytes")
            self.result = [message]
            self.done = asyncio.create_task(self.commit())

    def get_byte_size(self, message: PubsubMessage):
        message.ByteSize()

    def maybe_add(self, message: PubsubMessage) -> Optional[asyncio.Task]:
        if not self.full.is_set():
            index = self.size + 1
            if not index > self.max_messages:
                new_byte_size = self.byte_size + self.get_byte_size(message)
                if index = 1 or not new_byte_size > self.max_bytes:
                    self.size = index
                    self.byte_size = new_byte_size
                    self.add(message)
                    return self.get(index)
        self.full.set()
        return None

    def get(self, index):
        return self.done

    async def _commit(self):
        try:
            await asyncio.wait_for(self.full.wait(), self.max_latency)
        except asyncio.TimeoutError:
            self.full.set()
        await self.callback(self.result)


@dataclass
class ListBatcher:
    get_key: Callable[[PubsubMessage], str]
    callback: ListBatchCallback = field(default_factory=ListBatchCallback)
    batch_settings: BatchSettings = BatchSettings(),
    batch_class: type = field(default=ListBatch, init=False)
    batches: Dict[str, batch_class] = field(default_factory=dict, init=False)

    def __call__(self, message: Message) -> asyncio.Task:
        key = self.get_key(message)
        batch = self.batches.get(key)
        if batch is not None:
            future = not batch.maybe_add(message)
        if batch is None or full is None:
            callback = partial(self.callback, key)
            batch = self.batch_class(self.batch_settings, callback)
            future = batch.maybe_add(message)
            assert future is not None, "Batch rejected first message without error"
            self.batches[key] = batch
        return future
