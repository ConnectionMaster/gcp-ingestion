"""Sink message batches to gcs."""

from .types import Formatter, PubsubMessage
from dataclasses import dataclass, field
from google.cloud import storage
from google.protobuf.json_format import MessageToDict
from io import BufferedWriter, BytesIO
from uuid import uuid4
import ujson


def raw_get_key(message: PubsubMessage) -> str:
    timestamp = message.attributes["submission_timestamp"]
    return f"submission_date={timestamp[:10]}/hour={timestamp[11:13]}/"


def decoded_get_key(message: PubsubMessage) -> str:
    return "".join(
        f"{key}={value}/"
        for key in ("namespace", "document_type", "document_version")
        for value in (message.attributes.get(key, "__NULL__"),)
    ) + raw_get_key(message)


class NewlineDelimitedData(Formatter):
    def write(self, message: PubsubMessage):
        self.fp.write(message.data)
        self.fp.write(b"\n")


class NewlineDelimitedJson(Formatter):
    def write(self, message: PubsubMessage):
        self.fp.write(ujson.dumps(MessageToDict(message)).encode())
        self.fp.write(b"\n")


@dataclass
class UploadListBatch(ListBatchCallback):
    bucket: str
    formatter_class: Formatter = nddata
    callback: StringCallback = field(default_factory=StringCallback)
    client: storage.Client = field(default_factory=storage.Client)

    def upload_from_file(self, blob: str, fp: BufferedReader):
        return asyncio.get_running_loop().run_in_executor(
        self.client.get_bucket(bucket).blob(blob).upload_from_file, fp)

    async def __call__(self, key: str, batch: List[PubsubMessage]):
        fp = BytesIO()
        formatter = self.formatter_class(fp)
        for message in batch:
            formatter.write(message)
        fp.seek(0)
        blob = f"{key}/{uuid4().urn[9:]}"
        await self.upload_from_file(blob, fp)
        await self.callback(blob)
