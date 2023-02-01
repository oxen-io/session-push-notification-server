from threading import Thread
import asyncio
from tools.lokiLogger import LokiLogger


class BaseTask:
    def __init__(self):
        self.logger = LokiLogger().logger

        self.is_running = False
        self.thread = Thread(target=self.run_task)

    def run(self):
        self.logger.info(f'{self.__class__.__name__} start running...')
        self.is_running = True
        self.thread.start()

    def stop(self):
        self.logger.info(f'{self.__class__.__name__} stop running...')
        self.is_running = False

    def run_task(self):
        asyncio.run(self.create_task())

    async def create_task(self):
        while self.is_running:
            try:
                task = asyncio.create_task(self.task())
                await task
            except Exception as e:
                self.logger.exception(e)

    async def task(self):
        pass
