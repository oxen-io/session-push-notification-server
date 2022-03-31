from tasks.baseTask import *


class PushNotificationTask(BaseTask):
    def __init__(self):
        super().__init__()

        self.message_queue = Tools().message_queue
        self.notification_helper = Tools().notification_helper

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
