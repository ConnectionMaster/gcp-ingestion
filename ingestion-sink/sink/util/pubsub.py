"""Sink a pubsub subscription to gcs."""

from .types import FlowControl, MessageCallback
from google.cloud.pubsub import SubscriberClient
from typing import Any, Set
import asyncio
import google.auth
import time

MAX_ACK_DEADLINE = 600


@dataclass
class Subscribe:
    subscription: str
    callback: MessageCallback
    client: SubscriberClient = field(default_factory=SubscriberClient)
    flow_control: FlowControl = field(default_factory=FlowControl)
    ack_ids: Set[str] = field(default_factor=set, init=False)

    def __post_init__(self):
        if "/" not in self.subscription:
            _, project = google.auth.default()
            self.subscription = client.subscription_path(project, self.subscription)

    def in_thread(self, function: Callable, *args, **kwargs) -> Any:
        return asyncio.get_running_loop().run_in_executor(
            None, function, *args, **kwargs
        )

    async def hold_leases(self):
        next_attempt = 0
        while True:
            start_time = time.time()
            if self.ack_ids:
                try:
                    # extend the lease for every entry in ack_ids
                    self.in_thread(
                        self.client.api.modify_ack_deadline,
                        self.subscription,
                        self.ack_ids,
                        MAX_ACK_DEADLINE,
                    )
                except Exception:
                    pass  # ignore exceptions because we must not stop
                else:
                    # wait for half the time to run out before acking again
                    next_attempt = start_time + MAX_ACK_DEADLINE / 2
            # wait until next_attempt
            sleep_seconds = next_attempt - time.time()
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)

    def ack_or_nack(self, ack_id: str):
        async def callback(task: asyncio.Task):
            try:
                task.result()
            except:
                await self.in_thread(
                    client.api.modify_ack_deadline, self.subscription, [ack_id], 0
                )
                raise  # raise from bare except
            else:
                await self.in_thread(
                    self.client.api.acknowledge, self.subscription, ack_id
                )

        return callback

    async def __call__():
        asyncio.create_task(hold_leases())
        while True:
            request_batch_size = min(
                flow_control.max_request_batch_size,
                flow_control.max_messages - len(ack_ids),
            )
            if request_batch_size <= 0:
                await asyncio.sleep(flow_control.max_request_batch_latency)
            else:
                batch = await self.in_thread(
                    self.client.api.pull,
                    self.subscription,
                    request_batch_size,
                    return_immediately=False,
                )
                for item in batch.received_messages:
                    ack_id, message = item.ack_id, item.message
                    ack_ids.add(ack_id)
                    task = asyncio.create_task(callback(message))
                    task.add_done_callback(self.ack_or_nack(ack_id))
