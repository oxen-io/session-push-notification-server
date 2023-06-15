class HTTP:

    class Response:
        CODE = 'code'
        DATA = 'data'
        MSG = 'message'
        STATUS = 'status'
        HEADERS = 'headers'
        BODY = 'body'
        RESULT = 'result'

        SUCCESS = 'Success'
        FAIL = 'Fail'
        PARA_MISSING = 'Missing parameter'

    class RegistrationRequest:
        PUBKEY = 'pubKey'
        TOKEN = 'token'
        DEVICE_TYPE = 'device'

    class NotificationRequest:
        SEND_TO = 'send_to'
        DATA = 'data'

    class SubscriptionRequest:
        CLOSED_GROUP = 'closedGroupPublicKey'
        PUBKEY = 'pubKey'
        CLOSED_GROUPS = 'legacyGroupPublicKeys'

    class StatsDataRequest:
        START_DATE = 'start_date'
        END_DATE = 'end_date'
        IOS_PN_NUMBER = 'ios_pn_number'
        ANDROID_PN_NUMBER = 'android_pn_number'
        TOTAL_MESSAGE_NUMBER = 'total_message_number'
        CLOSED_GROUP_MESSAGE_NUMBER = 'closed_group_message_number'
        UNTRACKED_MESSAGE_NUMBER = 'untracked_message_number'
        DEDUPLICATED_ONE_ON_ONE_MESSAGE_NUMBER = 'deduplicated_1_1_message_number'

    class OnionRequest:
        CIPHERTEXT = 'ciphertext'
        EPHEMERAL = 'ephemeral_key'


class Environment:

    CERT_FILE = 'cert.pem'
    PRIVKEY_FILE = 'x25519-priv.pem'
    FIREBASE_TOKEN = 'loki-5a81e-firebase-adminsdk-7plup-0698317995.json'

    HUAWEI_APP_ID = "107146885"
    HUAWEI_APP_SECRET = "40da17ca27eab7565da0ce381bd6cf7690f9c442322b3939b4ea89dcec3a0602"

    debug_mode = True

