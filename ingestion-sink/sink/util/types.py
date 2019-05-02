"""Types."""

from google.cloud.pubsub_v1.types import PubsubMessage, FlowControl
from dataclasses import dataclass
from io import BufferedWriter
from typing import ClassVar


class Uri:
    prefix: ClassVar[str]
    value: str

    def __init__(self, value: str):
        self.value = value[self.prefix :]


class GcsUri(Uri):
    prefix = "gs://"

    def __init__(self, value: str):
        super().__init__(value)
        self.bucket = self.value


class PubsubUri(Uri):
    prefix = "pubsub://"


class StringCallback:
    """Base string callback does nothing."""

    async def __call__(self, value: str):
        pass


class MessageCallback:
    """Base message callback does nothing."""

    async def __call__(self, value: PubsubMessage):
        pass


class ListBatchCallback:
    """Base ListBatch callback does nothing."""

    async def __call__(self, key: str, value: List[PubsubMessage]):
        pass


@dataclass(frozen=True)
class BatchSettings:
    max_bytes: Optional[int] = None  # No maximum
    max_latency: Optional[int] = 10 * 60  # 10 minutes
    max_messages: Optional[int] = None  # No maximum
    # whether to raise ValueError if a single message exceeds max_bytes
    strict_max_bytes: bool = False

    def __post_init__(self):
        if (self.max_bytes, self.max_messages, self.max_latency) == (None, None, None):
            raise ValueError("BatchSettings requires at least one threshold")
        if self.max_messages is not None and not self.max_messages >= 1:
            raise ValueError(
                f"BatchSettings.max_messages must be >= 1," " got: {self.max_messages}"
            )


class Formatter:
    def __init__(self, fp: BufferedWriter):
        self.fp = fp

    def write(self, message: PubsubMessage):
        raise NotImplementedError(f"{cls.__name__}.write")
