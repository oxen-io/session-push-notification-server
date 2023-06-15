import signal
import urllib3
import resource
import json
import http
import logging
import asyncio
import uvloop

from flask import Flask, request, jsonify, abort
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

from taskRunner import TaskRunner
from const import *
from utils import decrypt, encrypt, make_symmetric_key, onion_request_data_handler, onion_request_v4_data_handler, DeviceType, is_ios_device_token
from tools.lokiLogger import LokiLogger
from tools.databaseHelperV2 import DatabaseHelperV2
from tools.pushNotificationHandler import PushNotificationHelperV2
from crypto import parse_junk

resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))
urllib3.disable_warnings()
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


def handle_exit(sig, frame):
    runner.stop_tasks()
    loop.stop()
    raise SystemExit


app = Flask(__name__)
auth = HTTPBasicAuth()
password_hash = generate_password_hash("^nfe+Lv+2d-2W!B8A+E-rdy^UJmq5#8D")  # your password
loop = IOLoop.instance()
signal.signal(signal.SIGTERM, handle_exit)

runner = TaskRunner()


def register_v2(args):
    device_token = None
    session_id = None
    if HTTP.RegistrationRequest.TOKEN in args:
        device_token = args[HTTP.RegistrationRequest.TOKEN]
    if HTTP.RegistrationRequest.PUBKEY in args:
        session_id = args[HTTP.RegistrationRequest.PUBKEY]
    if HTTP.RegistrationRequest.DEVICE_TYPE in args:
        device_type = DeviceType(args[HTTP.RegistrationRequest.DEVICE_TYPE])
    else:
        device_type = DeviceType.iOS if is_ios_device_token(device_token) else DeviceType.Android

    if device_token and session_id:
        PushNotificationHelperV2().register(device_token, session_id, device_type)
        return 1, HTTP.Response.SUCCESS
    else:
        LokiLogger().logger.info("Onion routing register error")
        raise Exception(HTTP.Response.PARA_MISSING)


def unregister(args):
    device_token = None
    if HTTP.RegistrationRequest.TOKEN in args:
        device_token = args[HTTP.RegistrationRequest.TOKEN]

    if device_token:
        session_id = PushNotificationHelperV2().unregister(device_token)
        if session_id:
            return 1, HTTP.Response.SUCCESS
        else:
            return 0, "Session id was not registered before."
    else:
        LokiLogger().logger.info("Onion routing unregister error")
        raise Exception(HTTP.Response.PARA_MISSING)


def register_legacy_groups_only(args):
    device_token = None
    session_id = None
    closed_group_ids = []
    if HTTP.RegistrationRequest.TOKEN in args:
        device_token = args[HTTP.RegistrationRequest.TOKEN]
    if HTTP.RegistrationRequest.PUBKEY in args:
        session_id = args[HTTP.RegistrationRequest.PUBKEY]
    if HTTP.RegistrationRequest.DEVICE_TYPE in args:
        device_type = DeviceType(args[HTTP.RegistrationRequest.DEVICE_TYPE])
    else:
        device_type = DeviceType.iOS if is_ios_device_token(device_token) else DeviceType.Android

    if HTTP.SubscriptionRequest.CLOSED_GROUPS in args:
        closed_group_ids = args[HTTP.SubscriptionRequest.CLOSED_GROUPS]



def subscribe_closed_group(args):
    closed_group_id = None
    session_id = None
    if HTTP.SubscriptionRequest.PUBKEY in args:
        session_id = args[HTTP.SubscriptionRequest.PUBKEY]
    if HTTP.SubscriptionRequest.CLOSED_GROUP in args:
        closed_group_id = args[HTTP.SubscriptionRequest.CLOSED_GROUP]

    if closed_group_id and session_id:
        PushNotificationHelperV2().subscribe_closed_group(closed_group_id, session_id)
        return 1, HTTP.Response.SUCCESS
    else:
        LokiLogger().logger.info("Onion routing subscribe closed group error")
        raise Exception(HTTP.Response.PARA_MISSING)


def unsubscribe_closed_group(args):
    closed_group_id = None
    session_id = None
    if HTTP.SubscriptionRequest.PUBKEY in args:
        session_id = args[HTTP.SubscriptionRequest.PUBKEY]
    if HTTP.SubscriptionRequest.CLOSED_GROUP in args:
        closed_group_id = args[HTTP.SubscriptionRequest.CLOSED_GROUP]

    if closed_group_id and session_id:
        closed_group = PushNotificationHelperV2().unsubscribe_closed_group(closed_group_id, session_id)
        if closed_group:
            return 1, HTTP.Response.SUCCESS
        else:
            return 0, "Cannot find the closed group id on PN server."
    else:
        LokiLogger().logger.info("Onion routing unsubscribe closed group error")
        raise Exception(HTTP.Response.PARA_MISSING)


def notify(args):
    session_id = None
    data = None
    if HTTP.NotificationRequest.SEND_TO in args:
        session_id = args[HTTP.NotificationRequest.SEND_TO]
    if HTTP.NotificationRequest.DATA in args:
        data = args[HTTP.NotificationRequest.DATA]

    if session_id and data:
        PushNotificationHelperV2().add_message_to_queue(args)
        return 1, HTTP.Response.SUCCESS
    else:
        raise Exception(HTTP.Response.PARA_MISSING)


Routing = {'register': register_v2,
           'unregister': unregister,
           'register_legacy_groups_only': register_legacy_groups_only,
           'subscribe_closed_group': subscribe_closed_group,
           'unsubscribe_closed_group': unsubscribe_closed_group,
           'notify': notify}


def onion_request_v4_body_handler(parameters):
    try:
        endpoint = parameters['endpoint']
        if endpoint.startswith('/'):
            endpoint = endpoint[1:]

        if Environment.debug_mode:
            LokiLogger().logger.info(parameters)
        func = Routing[endpoint]
        code, message = func(parameters)
        body = json.dumps(
            {
                HTTP.Response.CODE: code,
                HTTP.Response.MSG: message
            }
        )
        response = json.dumps(
            {
                HTTP.Response.CODE: 200,
                HTTP.Response.HEADERS: {'content-type': 'application/json'}
            }
        )

    except Exception as e:
        LokiLogger().logger.error(e)
        body = json.dumps(
            {
                HTTP.Response.CODE: 0,
                HTTP.Response.MSG: str(e)
            }
        )
        response = json.dumps(
            {
                HTTP.Response.CODE: 400,
                HTTP.Response.HEADERS: {'content-type': 'application/json'}
            }
        )

    v4response = b''.join(
        (b'l', str(len(response)).encode(), b':', response.encode(), str(len(body)).encode(), b':', body.encode(), b'e')
    )
    return v4response


def onion_request_body_handler(body):
    ciphertext = None
    ephemeral_pubkey = None
    symmetric_key = None
    response = json.dumps(
        {
            HTTP.Response.STATUS: 400,
            HTTP.Response.BODY: {
                HTTP.Response.CODE: 0,
                HTTP.Response.MSG: HTTP.Response.PARA_MISSING
            }
        }
    )
    if HTTP.OnionRequest.CIPHERTEXT in body:
        ciphertext = body[HTTP.OnionRequest.CIPHERTEXT]
    if HTTP.OnionRequest.EPHEMERAL in body:
        ephemeral_pubkey = body[HTTP.OnionRequest.EPHEMERAL]

    if ephemeral_pubkey:
        symmetric_key = make_symmetric_key(ephemeral_pubkey)
    else:
        LokiLogger().logger.error("Client public key is None.")
        LokiLogger().logger.error(f"This request is from {request.environ.get('HTTP_X_REAL_IP')}.")
        abort(400)

    if ciphertext and symmetric_key:
        try:
            parameters = json.loads(decrypt(ciphertext, symmetric_key).decode('utf-8'))
            args = json.loads(parameters['body'])
            if Environment.debug_mode:
                LokiLogger().logger.info(parameters)
            func = Routing[parameters['endpoint']]
            code, message = func(args)
            response = json.dumps(
                {
                    HTTP.Response.STATUS: 200,
                    HTTP.Response.BODY: {
                        HTTP.Response.CODE: code,
                        HTTP.Response.MSG: message
                    }
                }
            )
        except Exception as e:
            LokiLogger().logger.error(e)
            response = json.dumps(
                {
                    HTTP.Response.STATUS: 400,
                    HTTP.Response.BODY: {
                        HTTP.Response.CODE: 0,
                        HTTP.Response.MSG: str(e)
                    }
                }
            )
    else:
        LokiLogger().logger.error("Ciphertext or symmetric key is None.")
        abort(400)
    return jsonify({HTTP.Response.RESULT: encrypt(response, symmetric_key)})


@app.route('/loki/v2/lsrpc', methods=['POST'])
def onion_request_v2():
    body = {}
    if request.data:
        body = onion_request_data_handler(request.data)
    else:
        LokiLogger().logger.error(request.form)
    return onion_request_body_handler(body)


@app.route('/oxen/v4/lsrpc', methods=['POST'])
def onion_request_v4():
    junk = None

    try:
        junk = parse_junk(request.data)
    except RuntimeError as e:
        app.logger.warning("Failed to decrypt onion request: {}".format(e))
        abort(http.HTTPStatus.BAD_REQUEST)
    body = {}

    if junk:
        body = onion_request_v4_data_handler(junk)
    else:
        LokiLogger().logger.error(request.form)

    v4response = onion_request_v4_body_handler(body)

    return junk.transformReply(v4response)


@auth.verify_password
def verify_password(username, password):
    return check_password_hash(password_hash, password)


@app.route('/get_statistics_data', methods=['POST'])
@auth.login_required
def get_statistics_data():
    if auth.current_user():
        start_date = request.json.get(HTTP.StatsDataRequest.START_DATE)
        end_date = request.json.get(HTTP.StatsDataRequest.END_DATE)
        total_num_include = request.json.get(HTTP.StatsDataRequest.TOTAL_MESSAGE_NUMBER)
        ios_pn_num_include = request.json.get(HTTP.StatsDataRequest.IOS_PN_NUMBER)
        android_pn_num_include = request.json.get(HTTP.StatsDataRequest.ANDROID_PN_NUMBER)
        closed_group_message_include = request.json.get(HTTP.StatsDataRequest.CLOSED_GROUP_MESSAGE_NUMBER)
        keys_to_remove = []
        if total_num_include is not None and int(total_num_include) == 0:
            keys_to_remove.append(HTTP.StatsDataRequest.TOTAL_MESSAGE_NUMBER)
        if ios_pn_num_include is not None and int(ios_pn_num_include) == 0:
            keys_to_remove.append(HTTP.StatsDataRequest.IOS_PN_NUMBER)
        if android_pn_num_include is not None and int(android_pn_num_include) == 0:
            keys_to_remove.append(HTTP.StatsDataRequest.ANDROID_PN_NUMBER)
        if closed_group_message_include is not None and int(closed_group_message_include) == 0:
            keys_to_remove.append(HTTP.StatsDataRequest.CLOSED_GROUP_MESSAGE_NUMBER)

        data = DatabaseHelperV2().get_stats_data(start_date, end_date)
        for item in data:
            for key in keys_to_remove:
                item.pop(key, None)
        return jsonify(
            {
                HTTP.Response.CODE: 0,
                HTTP.Response.DATA: data
            }
        )


if __name__ == '__main__':
    app.logger.disabled = True
    logging.getLogger('werkzeug').disabled = not Environment.debug_mode
    logging.getLogger('tornado.access').disabled = not Environment.debug_mode
    runner.run_tasks()
    port = 3000 if Environment.debug_mode else 5000
    http_server = HTTPServer(WSGIContainer(app), no_keep_alive=True)
    http_server.listen(port)
    loop.start()
