from flask import Flask, request, jsonify
from pushNotificationHandler import SilentPushNotificationHelper
from const import *
from gevent.pywsgi import WSGIServer

app = Flask(__name__)
helper = SilentPushNotificationHelper()


@app.route('/register', methods=[GET, POST])
def register():
    token = None
    response = jsonify({CODE: 0,
                        MSG: PARA_MISSING})

    if TOKEN in request.args:
        token = request.args[TOKEN]

    if request.json and TOKEN in request.json:
        token = request.json[TOKEN]

    if request.form and TOKEN in request.form:
        token = request.form[TOKEN]

    if token:
        helper.update_token(token)
        response = jsonify({CODE: 1,
                            MSG: SUCCESS})
    return response


if __name__ == '__main__':
    helper.run()
    # app.run(host='0.0.0.0')
    http_server = WSGIServer(('', 5000), app)
    http_server.serve_forever()
    helper.stop()

