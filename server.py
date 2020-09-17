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


@app.route('/register', methods=[GET, POST])
def register():
    device_token = None
    session_id = None
    response = jsonify({CODE: 0,
                        MSG: PARA_MISSING})

    if TOKEN in request.args:
        device_token = request.args[TOKEN]
        if PUBKEY in request.args:
            session_id = request.args[PUBKEY]

    if request.json and TOKEN in request.json:
        device_token = request.json[TOKEN]
        if PUBKEY in request.json:
            session_id = request.json[PUBKEY]

    if request.form and TOKEN in request.form:
        device_token = request.form[TOKEN]
        if PUBKEY in request.form:
            session_id = request.form[PUBKEY]

    if device_token and session_id:
        PN_helper.register(device_token, session_id)
        response = jsonify({CODE: 1,
                            MSG: SUCCESS})
    elif device_token:
        PN_helper.unregister(device_token)
        response = jsonify({CODE: 1,
                            MSG: SUCCESS})
    return response


@app.route('/subscribe_closed_group', methods=[GET, POST])
def subscribe_to_closed_group():
    closed_group_id = None
    session_id = None
    response = jsonify({CODE: 0,
                        MSG: PARA_MISSING})

    if PUBKEY in request.args:
        session_id = request.args[PUBKEY]
        if CLOSED_GROUP in request.args:
            closed_group_id = request.args[CLOSED_GROUP]

    if request.json and PUBKEY in request.json:
        session_id = request.json[PUBKEY]
        if CLOSED_GROUP in request.json:
            closed_group_id = request.json[CLOSED_GROUP]

    if request.form and PUBKEY in request.form:
        session_id = request.form[PUBKEY]
        if CLOSED_GROUP in request.form:
            closed_group_id = request.form[CLOSED_GROUP]

    if closed_group_id and session_id:
        PN_helper.subscribe_closed_group(closed_group_id, session_id)
        response = jsonify({CODE: 1,
                            MSG: SUCCESS})
    return response


@app.route('/unsubscribe_closed_group', methods=[GET, POST])
def unsubscribe_to_closed_group():
    closed_group_id = None
    session_id = None
    response = jsonify({CODE: 0,
                        MSG: PARA_MISSING})

    if PUBKEY in request.args:
        session_id = request.args[PUBKEY]
        if CLOSED_GROUP in request.args:
            closed_group_id = request.args[CLOSED_GROUP]

    if request.json and PUBKEY in request.json:
        session_id = request.json[PUBKEY]
        if CLOSED_GROUP in request.json:
            closed_group_id = request.json[CLOSED_GROUP]

    if request.form and PUBKEY in request.form:
        session_id = request.form[PUBKEY]
        if CLOSED_GROUP in request.form:
            closed_group_id = request.form[CLOSED_GROUP]

    if closed_group_id and session_id:
        PN_helper.unsubscribe_closed_group(closed_group_id, session_id)
        response = jsonify({CODE: 1,
                            MSG: SUCCESS})
    return response


@app.route('/loki/v1/lsrpc', methods=[POST])
def onion_reqeust():
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
            if parameters['endpoint'] == 'notify':
                PN_helper.add_message_to_queue(parameters['body'])
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

