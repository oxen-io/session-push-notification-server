import flask
from . import config
import coloredlogs
import uwsgi
import oxenmq
from uwsgidecorators import postfork

app = flask.Flask(__name__)
coloredlogs.install(
    milliseconds=True, isatty=True, logger=app.logger, level=config.core_logger.get_level()
)

# Monkey-patch app.get/post/etc. for Flask <2 compatibility; this has to be before the imports,
# below, because they depend on this existing.
if not hasattr(flask.Flask, "post"):

    def _add_route_shortcut(on, name):
        def meth(self, rule: str, **options):
            return self.route(rule, methods=[name.upper()], **options)

        setattr(on, name, meth)

    for method in ("get", "post", "put", "delete", "patch"):
        _add_route_shortcut(flask.Flask, method)
        _add_route_shortcut(flask.Blueprint, method)

omq = None
hivemind = None


@postfork
def start_oxenmq():
    if uwsgi.mule_id() != 0:
        # Mules manage their own connections
        return

    global omq, hivemind

    app.logger.info(f"Starting oxenmq connection from web worker {uwsgi.worker_id()}")

    omq = oxenmq.OxenMQ()
    omq.start()
    app.logger.info("Started web worker, connecting to hivemind")

    hivemind = omq.connect_remote(oxenmq.Address(config.config.hivemind_sock))


# Load components that depend on our `app` for registering themselves:
from . import onion_request
from . import register
