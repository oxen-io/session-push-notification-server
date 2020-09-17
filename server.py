from flask import Flask, request, jsonify
from pushNotificationHandler import PushNotificationHelper
from const import *
from lokiLogger import LokiLogger
import urllib3
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
import resource
from utils import decrypt, encrypt, make_symmetric_key
import json


resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))
urllib3.disable_warnings()

app = Flask(__name__)
logger = LokiLogger().logger
PN_helper = PushNotificationHelper(logger)


def register(args):
    device_token = None
    session_id = None
    if TOKEN in args:
        device_token = args[TOKEN]
    if PUBKEY in args:
        session_id = args[PUBKEY]

    if device_token and session_id:
        PN_helper.register(device_token, session_id)
    else:
        raise Exception(PARA_MISSING)


def unregister(args):
    device_token = None
    if TOKEN in args:
        device_token = args[TOKEN]

    if device_token:
        PN_helper.unregister(device_token)
    else:
        raise Exception(PARA_MISSING)


def subscribe_to_closed_group(args):
    closed_group_id = None
    session_id = None
    if PUBKEY in args:
        session_id = args[PUBKEY]
    if CLOSED_GROUP in args:
        closed_group_id = args[CLOSED_GROUP]

    if closed_group_id and session_id:
        PN_helper.subscribe_closed_group(closed_group_id, session_id)
    else:
        raise Exception(PARA_MISSING)


def unsubscribe_to_closed_group(args):
    closed_group_id = None
    session_id = None
    if PUBKEY in args:
        session_id = request.args[PUBKEY]
    if CLOSED_GROUP in request.args:
        closed_group_id = args[CLOSED_GROUP]

    if closed_group_id and session_id:
        PN_helper.unsubscribe_closed_group(closed_group_id, session_id)
    else:
        raise Exception(PARA_MISSING)


def notify(args):
    session_id = None
    data = None
    if SEND_TO in args:
        session_id = args[SEND_TO]
    if DATA in args:
        data = args[DATA]

    if session_id and data:
        PN_helper.add_message_to_queue(args)
    else:
        raise Exception(PARA_MISSING)


Routing = {'register': register,
           'unregister': unregister,
           'subscribe_to_closed_group': subscribe_to_closed_group,
           'unsubscribe_to_closed_group': unsubscribe_to_closed_group,
           'notify': notify}


@app.route('/loki/v1/lsrpc', methods=[POST])
def onion_request():
    ciphertext = None
    ephemeral_pubkey = None
    response = json.dumps({STATUS: 400,
                           BODY: {CODE: 0,
                                  MSG: PARA_MISSING}})

    if request.data:
        body_as_string = request.data.decode('utf-8')
        body = json.loads(body_as_string)
        if CIPHERTEXT in body:
            ciphertext = body[CIPHERTEXT]
        if EPHEMERAL in body:
            ephemeral_pubkey = body[EPHEMERAL]

    symmetric_key = make_symmetric_key(ephemeral_pubkey)

    if ciphertext and symmetric_key:
        try:
            parameters = json.loads(decrypt(ciphertext, symmetric_key).decode('utf-8'))
            args = json.dumps(parameters['body'])
            func = Routing[parameters['endpoint']]
            func(args)
            response = json.dumps({STATUS: 200,
                                   BODY: {CODE: 1,
                                          MSG: SUCCESS}})
        except Exception as e:
            logger.error(e)
            response = json.dumps({STATUS: 400,
                                   BODY: {CODE: 0,
                                          MSG: str(e)}})
    return jsonify({RESULT: encrypt(response, symmetric_key)})


if __name__ == '__main__':
    PN_helper.run()
    port = 4000  # for stage server
    http_server = HTTPServer(WSGIContainer(app), no_keep_alive=True)
    http_server.listen(port)
    IOLoop.instance().start()
    PN_helper.stop()

