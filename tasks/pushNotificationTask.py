from tasks.baseTask import *
from tools.pushNotificationHandler import PushNotificationHelperV2


class PushNotificationTask(BaseTask):
    def __init__(self):
        super().__init__()
        self.notification_helper = None # Create the instance in the runLoop

    async def task(self):
        self.notification_helper = PushNotificationHelperV2()

        while self.is_running:
            await self.notification_helper.send_push_notification()
            await asyncio.sleep(0.5)
