import asyncio

from prefect import State

from src.prefect.client.orchestration import PrefectClient
from src.prefect.client.schemas.filters import FlowFilter, FlowRunFilter, FlowRunFilterState


async def main():
    client = PrefectClient(api="http://localhost:4200/api")
    # all runs that are running
    running = await client.read_flow_runs(
        flow_run_filter=FlowRunFilter(
            state=FlowRunFilterState(type={"any_": ["RUNNING"]})
        )
    )

    print(running)
    for r in running:
        print(f"Cancelling flow run {r.id} ({r.name})")
        await client.set_flow_run_state(r.id, State(type="CANCELLING"))
        # await client.delete_flow_run(r.id)
    # Cancel all running flows concurrently
    # await asyncio.gather(*[
    #     asyncio.to_thread(client.cancel_flow_run, run.id)
    #     for run in running
    # ])

if __name__ == "__main__":
    asyncio.run(main())
