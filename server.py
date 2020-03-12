from flask import Flask, request, jsonify
from pushNotificationHandler import SilentPushNotificationHelper, NormalPushNotificationHelper
from const import *
from gevent.pywsgi import WSGIServer

app = Flask(__name__)
SPN_helper = SilentPushNotificationHelper()
NPN_helper = NormalPushNotificationHelper()


@app.route('/register', methods=[GET, POST])
def register():
    token = None
    pubkey = None
    response = jsonify({CODE: 0,
                        MSG: PARA_MISSING})

    if TOKEN in request.args:
        token = request.args[TOKEN]
        if PUBKEY in request.args:
            pubkey = request.args[TOKEN]

    if request.json and TOKEN in request.json:
        token = request.json[TOKEN]
        if PUBKEY in request.json:
            pubkey = request.json[TOKEN]

    if request.form and TOKEN in request.form:
        token = request.form[TOKEN]
        if PUBKEY in request.form:
            pubkey = request.form[TOKEN]

    if token and pubkey:
        NPN_helper.update_token_pubkey_pair(token, pubkey)
        response = jsonify({CODE: 1,
                            MSG: SUCCESS})
    elif token:
        SPN_helper.update_token(token)
        response = jsonify({CODE: 1,
                            MSG: SUCCESS})
    return response


if __name__ == '__main__':
    SPN_helper.run()
    NPN_helper.run()
    port = 3000 if debug_mode else 5000
    http_server = WSGIServer(('', port), app)
    http_server.serve_forever()
    SPN_helper.stop()
    NPN_helper.stop()

