from aggregate_settings import *
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


class Aggregate:
    def __init__(self, aggregate_stream_name, collection_name, subscription_name, watched_stream_name):
        """
        :param aggregate_stream_name: The name of the stream this aggregate will post its events to
        :param collection_name: The name of the collection in mongodb which will store events
        :param subscription_name: Name of the eventstore subscription
        :param watched_stream_name: Name of the event stream which will be watched to create an aggregate
        """
        log.trace("enter Aggregate.__init__()")
        self.aggregate_stream_name = aggregate_stream_name
        log.debug(f"self.aggregate_stream_name = {self.aggregate_stream_name}")
        self.collection_name = collection_name
        log.debug(f"self.collection_name = {self.collection_name}")
        self.subscription_name = subscription_name
        log.debug(f"self.subscription_name = {self.subscription_name}")
        self.watched_stream_name = watched_stream_name
        log.debug(f"self.watched_stream_name = {self.watched_stream_name}")
        log.trace("exit Aggregate.__init__()")

    @run_in_executor
    def post_to_aggregate_stream(self, event_id, result, event_type):
        log.trace("enter Aggregate.post_to_aggregate_stream()")
        headers = {
            "ES-EventType": event_type,
            "ES-EventId": str(uuid.uuid1())
        }
        log.debug(f"headers = {headers}")
        try:
            the_json = {"event_id": str(event_id), "result": result}
            requests.post(
                "http://%s:%s/streams/%s" % (EVENT_STORE_URL, EVENT_STORE_HTTP_PORT, self.aggregate_stream_name),
                headers=headers,
                json=the_json
            )
            log.debug(f"the_json = {the_json}")
        except KeyError as e:
            log.error(f"key missing from event: {e}")
        log.trace("exit Aggregate.post_to_aggregate_stream()")

    @run_in_executor
    def update_backend(self, event):
        log.trace("enter Aggregate.update_backend()")
        client = MongoClient('mongodb://%s:%s@%s' % (MONGO_USER, MONGO_PASS, MONGO_URL), MONGO_PORT)
        db = client["wigglybot_db"]
        dialogues = db.db[self.collection_name]
        log.trace("exit Aggregate.update_backend()")
        return str(dialogues.update_one({'event_id': event["event_id"]}, {"$set": event}, upsert=True).raw_result)

    async def create_subscription(self, conn):
        log.trace("enter Aggregate.create_subscription()")
        await conn.create_subscription(self.subscription_name, self.watched_stream_name)
        log.trace("exit Aggregate.create_subscription()")

    async def aggregate_fn(self):
        log.trace("enter Aggregate.aggregate_fn()")
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
                await self.create_subscription(c)
            except exceptions.SubscriptionCreationFailed as e:
                if e.message.find("already exists"):
                    log.info(f"{self.subscription_name} {self.watched_stream_name} subscription found.")
                else:
                    log.exception(e)
            dialogue_stream = await c.connect_subscription(self.subscription_name, self.watched_stream_name)

            async for event in dialogue_stream.events:
                event_obj = json.loads(event.event.data)
                log.debug("aggregate_fn() responding to: %s" % json.dumps(event_obj))
                try:
                    await self.post_to_aggregate_stream(
                        str(event_obj["event_id"]),
                        await self.update_backend(event_obj),
                        event.type
                    )
                    await dialogue_stream.ack(event)
                except KeyError as e:
                    log.error(f"key missing from event: {e}")
                except Exception as e:
                    log.exception(e)
        log.trace("exit Aggregate.aggregate_fn()")
