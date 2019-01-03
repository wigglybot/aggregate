from settings import *
import asyncio
from photonpump import connect, exceptions
import json
import functools
from pymongo import MongoClient
import uuid
import requests


def run_in_executor(f):
    """
    wrap a blocking (non-asyncio) func so it is executed in our loop
    """
    @functools.wraps(f)
    def inner(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_in_executor(None, functools.partial(f, *args, **kwargs))
    return inner


@run_in_executor
def post_to_aggregate_stream(event_id, result, event_type):
    headers = {
        "ES-EventType": event_type,
        "ES-EventId": str(uuid.uuid1())
    }
    requests.post(
        "http://%s:%s/streams/aggregate" % (EVENT_STORE_URL, EVENT_STORE_HTTP_PORT),
        headers=headers,
        json={"event_id": str(event_id), "result": result}
    )


@run_in_executor
def update_backend(event):
    client = MongoClient('mongodb://%s:%s@%s' % (MONGO_USER, MONGO_PASS, MONGO_URL), MONGO_PORT)
    wigglybot_db = client["wigglybot_db"]
    dialogues = wigglybot_db.db['dialogues']
    return str(dialogues.update_one({'event_id': event["event_id"]}, {"$set": event}, upsert=True).raw_result)


async def create_subscription(subscription_name, stream_name, conn):
    await conn.create_subscription(subscription_name, stream_name)


async def aggregate_fn():
    _loop = asyncio.get_event_loop()
    async with connect(
            host=EVENT_STORE_URL,
            port=EVENT_STORE_TCP_PORT,
            username=EVENT_STORE_USER,
            password=EVENT_STORE_PASS,
            loop=_loop
    ) as c:
        await c.connect()
        try:
            await create_subscription("aggregate", "dialogue", c)
        except exceptions.SubscriptionCreationFailed as e:
            if e.message.find("'aggregate' already exists."):
                log.info("Aggregate dialogue subscription found.")
            else:
                raise e
        dialogue_stream = await c.connect_subscription("aggregate", "dialogue")
        async for event in dialogue_stream.events:
            event_obj = json.loads(event.event.data)
            log.debug("aggregate_fn() responding to: %s" % json.dumps(event_obj))
            try:
                await post_to_aggregate_stream(str(event_obj["event_id"]), await update_backend(event_obj), event.type)
                await dialogue_stream.ack(event)
            except Exception as e:
                log.exception(e)


if __name__ == "__main__":
    asyncio.set_event_loop(asyncio.new_event_loop())
    mainloop = asyncio.get_event_loop()
    mainloop.run_until_complete(aggregate_fn())
