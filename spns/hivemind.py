from .core import HiveMind, logger as core_logger
from . import config
from .config import logger
import time
import signal
import os


def run():
    """Runs a HiveMind instance indefinitely"""

    hivemind = None

    def stop(*args):
        nonlocal hivemind
        if hivemind:
            logger.info("Shutting down hivemind")
            hivemind.stop()
            logger.info("Hivemind stopped")
            hivemind = None

    def sig_die(signum, frame):
        raise OSError(f"Caught signal {signal.Signals(signum).name}")

    try:
        logger.info("Starting hivemind")
        core_logger.start("stderr")
        hivemind = HiveMind(config.config)
        logger.info("Hivemind started")

        signal.signal(signal.SIGHUP, sig_die)
        signal.signal(signal.SIGINT, sig_die)

        while True:
            time.sleep(3600)
    except Exception as e:
        logger.critical(f"HiveMind died: {e}")

    if hivemind:
        stop()


if __name__ == "__main__":
    run()
