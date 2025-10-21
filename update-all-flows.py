import asyncio

from prefect import State

from src.prefect.client.orchestration import PrefectClient
from src.prefect.client.schemas.filters import FlowFilter, FlowRunFilter, FlowRunFilterState
from pprint import pprint


async def main():
    client = PrefectClient(api="http://localhost:4200/api")
    running = await client.read_deployments(
    )

    running = [r for r in running if r.name.startswith("sample-dask-dev")]
    pprint(running)

    r = running[0]
    if r.job_variables:
        if 'spec' in r.job_variables \
            and 'template' in r.job_variables['spec'] \
            and 'spec' in r.job_variables['spec']['template'] \
            and 'containers' in r.job_variables['spec']['template']['spec'] \
            and 'env' in r.job_variables['spec']['template']['spec']['containers'][0]:

            env = r.job_variables['spec']['template']['spec']['containers'][0]['env']
            # remove all keys starting with PREFECT_LOGGING_
            env = [e for e in env if not e['name'].startswith('PREFECT_LOGGING_')]
            r.job_variables['spec']['template']['spec']['containers'][0]['env'] = env
            print('removed PREFECT_LOGGING_ env vars')

            await client.update_deployment(
                deployment_id=r.id,
                deployment=r
            )

    print(len(running))

    # for r in running:
    #     print(f"Cancelling flow run {r.id} ({r.name})")
    #     await client.set_flow_run_state(r.id, State(type="CANCELLING"))
        # await client.delete_flow_run(r.id)
    # Cancel all running flows concurrently
    # await asyncio.gather(*[
    #     asyncio.to_thread(client.cancel_flow_run, run.id)
    #     for run in running
    # ])

if __name__ == "__main__":
    asyncio.run(main())
