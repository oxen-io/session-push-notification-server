from tasks.baseTask import *
from tools.pushNotificationHandler import PushNotificationHelperV2


class PushNotificationTask(BaseTask):
    def __init__(self):
        super().__init__()

        self.notification_helper = PushNotificationHelperV2()
        self.message_queue = self.notification_helper.message_queue

    async def task(self):
        while not self.is_running:
            if self.message_queue.empty():
                return
            # Get at most 1000 messages every second
            messages_wait_to_push = []
            while not self.message_queue.empty() or len(messages_wait_to_push) > 1000:
                messages_wait_to_push.append(self.message_queue.get())
            self.notification_helper.send_push_notification(messages_wait_to_push)
            await asyncio.sleep(0.5)
