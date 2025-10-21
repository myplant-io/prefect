from datetime import timedelta

from prefect.automations import Automation
from prefect.blocks.notifications import CustomWebhookNotificationBlock
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.objects import StateType
from prefect.events.actions import ChangeFlowRunState, SendNotification
from prefect.events.schemas.automations import EventTrigger, Posture, AutomationCore
from prefect.events.schemas.events import ResourceSpecification

# TODO: Replace with your actual Microsoft Teams webhook URL
TEAMS_WEBHOOK_URL = "https://innio.webhook.office.com/webhookb2/db84d625-b575-49f3-8a23-90e706884f6f@818a9cf9-7d6e-4339-926b-9931bc704ef7/IncomingWebhook/246e8b4560dd4a86aad1ec934469aade/8361b167-6f12-47e1-95e1-df932f3cead4/V2sPvU1gRzixUarsWsu7pgYL5otCWZp_jCWG_b3Sw8pD01"
BLOCK_NAME = "zombie-flow-alerts"


async def get_or_create_teams_block(client) -> CustomWebhookNotificationBlock:
    """Load existing Teams webhook block or create a new one."""
    try:
        return await CustomWebhookNotificationBlock.aload(BLOCK_NAME, client=client)
    except ValueError:
        block = CustomWebhookNotificationBlock(
            name="Teams Zombie Flow Alerts",
            url=TEAMS_WEBHOOK_URL,
            json_data={
                "title": "{{subject}}",
                "text": "{{body}}",
            },
        )
        await block.asave(BLOCK_NAME, client=client)
        return block


async def main():
    async with PrefectClient(api="http://localhost:4200/api") as client:
        teams_block = await get_or_create_teams_block(client)

        my_automation = Automation(
            name="Crash zombie flows",
            trigger=EventTrigger(
                after={"prefect.flow-run.heartbeat", "prefect.flow-run.Running"},
                expect={
                    "prefect.flow-run.heartbeat",
                    "prefect.flow-run.Completed",
                    "prefect.flow-run.Failed",
                    "prefect.flow-run.Cancelled",
                    "prefect.flow-run.Crashed",
                },
                match=ResourceSpecification({"prefect.resource.id": ["prefect.flow-run.*"]}),
                for_each={"prefect.resource.id"},
                posture=Posture.Proactive,
                threshold=1,
                # within=timedelta(seconds=90), # TODO
                within=timedelta(seconds=15),
            ),
            actions=[
                # ChangeFlowRunState(
                #     state=StateType.CRASHED,
                #     message="Flow run marked as crashed due to missing heartbeats.",
                # ),
                SendNotification(
                    block_document_id=teams_block._block_document_id,
                    subject="Zombie Flow Detected",
                    body="""Flow run crashed due to missing heartbeats.                                                                                                                                          

        Flow Run ID: {{ flow_run.id }}                                                                                                                                                                           
        Flow Run Name: {{ flow_run.name }}                                                                                                                                                                       
        State: {{ flow_run.state.name }}                                                                                                                                                                         
        Timestamp: {{ flow_run.state.timestamp }}                                                                                                                                                                
        """,
                ),
            ],
        )
        automation = AutomationCore(**my_automation.model_dump(exclude={"id"}))
        await client.create_automation(automation=automation)
        # await client.create_automation(my_automation)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())