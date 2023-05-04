import flask
from . import config
import coloredlogs
import uwsgi
import oxenmq
from uwsgidecorators import postfork

app = flask.Flask(__name__)
coloredlogs.install(milliseconds=True, isatty=True, logger=app.logger, level=config.LOG_LEVEL)

omq = None
hivemind = None


@postfork
def start_oxenmq():
    if uwsgi.mule_id() != 0:
        # Mules manage their own connections
        return

    global omq, hivemind

    app.logger.debug(f"Starting oxenmq connection from web worker {uwsgi.worker_id()}")

    omq = oxenmq.OxenMQ()
    omq.start()
    app.logger.debug("Started, connecting to hivemind")

    hivemind = omq.connect_remote(oxenmq.Address(config.HIVEMIND_SOCK))


# Load components that depend on our `app` for registering themselves:
from . import onion_request
from . import register
