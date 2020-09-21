from flask import Flask, request, jsonify
from pushNotificationHandler import PushNotificationHelperV2
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

# PN approach V1 #
from pushNotificationHandler import SilentPushNotificationHelper, NormalPushNotificationHelper

SPN_helper = SilentPushNotificationHelper(logger)
NPN_helper = NormalPushNotificationHelper(logger)


@app.route('/register', methods=[GET, POST])
def register():
    token = None
    pubkey = None
    response = jsonify({CODE: 0,
                        MSG: PARA_MISSING})

    if TOKEN in request.args:
        token = request.args[TOKEN]
        if PUBKEY in request.args:
            pubkey = request.args[PUBKEY]

    if request.json and TOKEN in request.json:
        token = request.json[TOKEN]
        if PUBKEY in request.json:
            pubkey = request.json[PUBKEY]

    if request.form and TOKEN in request.form:
        token = request.form[TOKEN]
        if PUBKEY in request.form:
            pubkey = request.form[PUBKEY]

    if token and pubkey:
        NPN_helper.update_token_pubkey_pair(token, pubkey)
        SPN_helper.disable_token(token)
        response = jsonify({CODE: 1,
                            MSG: SUCCESS})
    elif token:
        SPN_helper.update_token(token)
        NPN_helper.disable_token(token)
        response = jsonify({CODE: 1,
                            MSG: SUCCESS})
    return response


@app.route('/acknowledge_message_delivery', methods=[GET, POST])
def update_last_hash():
    last_hash = None
    pubkey = None
    expiration = None
    response = jsonify({CODE: 0,
                        MSG: PARA_MISSING})

    if LASTHASH in request.args:
        last_hash = request.args[LASTHASH]
        if PUBKEY in request.args:
            pubkey = request.args[PUBKEY]
        if EXPIRATION in request.args:
            expiration = request.args[EXPIRATION]

    if request.json and LASTHASH in request.json:
        last_hash = request.json[LASTHASH]
        if PUBKEY in request.json:
            pubkey = request.json[PUBKEY]
        if EXPIRATION in request.json:
            expiration = request.json[EXPIRATION]

    if request.form and LASTHASH in request.form:
        last_hash = request.form[LASTHASH]
        if PUBKEY in request.form:
            pubkey = request.form[PUBKEY]
        if EXPIRATION in request.form:
            expiration = request.form[EXPIRATION]

    if last_hash and pubkey and expiration:
        NPN_helper.update_last_hash(pubkey, last_hash, expiration)
        response = jsonify({CODE: 1,
                            MSG: SUCCESS})

    return response


# PN approach V2 #
PN_helper_v2 = PushNotificationHelperV2(logger)


def register_v2(args):
    device_token = None
    session_id = None
    if TOKEN in args:
        device_token = args[TOKEN]
    if PUBKEY in args:
        session_id = args[PUBKEY]

    if device_token and session_id:
        PN_helper_v2.register(device_token, session_id)
        # PN approach V1 #
        NPN_helper.update_token_pubkey_pair(device_token, session_id)
        SPN_helper.disable_token(device_token)
    else:
        raise Exception(PARA_MISSING)


def unregister(args):
    device_token = None
    if TOKEN in args:
        device_token = args[TOKEN]

    if device_token:
        PN_helper_v2.unregister(device_token)
        # PN approach V1 #
        SPN_helper.update_token(device_token)
        NPN_helper.disable_token(device_token)
    else:
        raise Exception(PARA_MISSING)


def subscribe_closed_group(args):
    closed_group_id = None
    session_id = None
    if PUBKEY in args:
        session_id = args[PUBKEY]
    if CLOSED_GROUP in args:
        closed_group_id = args[CLOSED_GROUP]

    if closed_group_id and session_id:
        PN_helper_v2.subscribe_closed_group(closed_group_id, session_id)
    else:
        raise Exception(PARA_MISSING)


def unsubscribe_closed_group(args):
    closed_group_id = None
    session_id = None
    if PUBKEY in args:
        session_id = request.args[PUBKEY]
    if CLOSED_GROUP in request.args:
        closed_group_id = args[CLOSED_GROUP]

    if closed_group_id and session_id:
        PN_helper_v2.unsubscribe_closed_group(closed_group_id, session_id)
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
        PN_helper_v2.add_message_to_queue(args)
    else:
        raise Exception(PARA_MISSING)


Routing = {'register': register_v2,
           'unregister': unregister,
           'subscribe_closed_group': subscribe_closed_group,
           'unsubscribe_closed_group': unsubscribe_closed_group,
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
            args = json.loads(parameters['body'])
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
    SPN_helper.run()
    NPN_helper.run()
    PN_helper_v2.run()
    port = 3000 if debug_mode else 5000
    http_server = HTTPServer(WSGIContainer(app), no_keep_alive=True)
    http_server.listen(port)
    IOLoop.instance().start()
    SPN_helper.stop()
    NPN_helper.stop()
    PN_helper_v2.stop()
