from aggregate import Aggregate
import asyncio
from aggregate_settings import CONFIG


if __name__ == "__main__":
    asyncio.set_event_loop(asyncio.new_event_loop())
    mainloop = asyncio.get_event_loop()

    aggregate_tasks = []
    for aggregate in CONFIG["aggregates"]:
        new_aggregate = Aggregate(
            CONFIG["aggregates"][aggregate]["aggregate_stream_name"],
            CONFIG["aggregates"][aggregate]["collection_name"],
            CONFIG["aggregates"][aggregate]["subscription_name"],
            CONFIG["aggregates"][aggregate]["watched_stream_name"],
        )
        aggregate_tasks.append(new_aggregate.aggregate_fn())

    mainloop.run_until_complete(asyncio.wait(aggregate_tasks))
