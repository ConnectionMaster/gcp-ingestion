"""Copy messages from an input to an output."""

from .util import gcs, batch, pubsub
from .util.types import GcsUri, PubsubUri, Uri
import os

def parse_uri(value: str) -> Uri:
    if value.startwith(PubsubUri.prefix):
        return PubsubUri(value)
    elif value.startswith(GcsUri.prefix):
        return GcsUri(value)
    else:
        raise NotImplementedError(f"uri protocol not supported: {uri}")


def get_input(callback: MessageCallback):
    src = parse_uri(os.environ.get("INPUT", f"{PubsubUri.prefix}raw-landfill"))
    if isinstance(src, PubsubUri):
        return pubsub.subscribe(uri.value, callback)
    else:
        raise NotImplementedError(f"input protocol: {dest.prefix}")

batchers = {
    cls.__name__: cls
    for cls in (batch.ListBatcher,)
}
get_key_fns = {
    fn.__name__: fn
    for fn in (gcs.raw_get_key, gcs.decoded_get_key)
}
formatters = {
    cls.__name__: cls
    for cls in (gcs.NewlineDelimitedJson, NewlineDelimitedData)
}


def get_output():
    dest = parse_uri(os.environ.get("OUTPUT", GcsUri.prefix))
    if isinstance(dest, GcsUri):
        batcher_class = batchers[os.environ.get("BATCHER", next(batchers.keys()))]
        get_key = get_key_fns[os.environ.get("GET_KEY_FUNCTION", next(get_key_fns.keys()))]
        formatter_class = formatters[os.environ.get("FORMATTER", next(formatters.keys()))]
        if isinstance(batcher, ListBatcher):
            output = UploadListBatch(dest.bucket, formatter_class)
            return batcher(get_key, output)
        else:
            raise NotImplementedError(f"batcher: {batcher_class.__name__}")
    else:
        raise NotImplementedError(f"output protocol: {dest.prefix}")


output = get_output()

main = get_input(output)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
