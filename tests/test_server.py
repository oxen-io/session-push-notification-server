import unittest
from test_const import *
from server import *

tests_cases = ['lsrpc',
               'get_statistics_data',
               'register',
               'notify',
               'unregister',
               'subscribe_closed_group',
               'unsubscribe_closed_group']

database_helper.migrate_database_if_needed()
database_helper.load_cache()


class ServerTests(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()

    def tearDown(self):
        pass

    def test_0_lsrpc(self):
        body = {}
        body_as_string = json.dumps(body)
        ciphertext = b'...'
        ciphertext_length = len(ciphertext).to_bytes(4, "little")
        data = ciphertext_length + ciphertext + body_as_string.encode('utf-8')
        response = self.app.post('/loki/v2/lsrpc', data=data)
        self.assertEqual(response.status_code, 400)

    def test_1_get_statistics_data(self):
        header = {'Authorization': 'Basic dGVzdDpebmZlK0x2KzJkLTJXIUI4QStFLXJkeV5VSm1xNSM4RA==',
                  'Content-Type': 'application/json'}
        params = {}
        response = self.app.post('/get_statistics_data', headers=header, json=params)
        self.assertEqual(response.status_code, 200)

    def test_2_register(self):
        args = {TOKEN: TEST_TOKEN_0,
                PUBKEY: TEST_SESSION_ID}
        register_v2(args)
        test_device_in_cache = database_helper.device_cache.get(TEST_SESSION_ID)
        self.assertTrue(TEST_TOKEN_0 in test_device_in_cache.tokens)

    def test_3_notify(self):
        args = {SEND_TO: TEST_SESSION_ID,
                DATA: TEST_DATA}
        notify(args)
        message_in_queue = PN_helper_v2.message_queue.get()
        self.assertEqual(args, message_in_queue)

    def test_4_unregister(self):
        args = {TOKEN: TEST_TOKEN_0}
        unregister(args)
        test_device_in_cache = database_helper.device_cache.get(TEST_SESSION_ID)
        self.assertFalse(TEST_TOKEN_0 in test_device_in_cache.tokens)

    def test_5_subscribe_closed_group(self):
        args = {CLOSED_GROUP: TEST_CLOSED_GROUP_ID,
                PUBKEY: TEST_SESSION_ID}
        subscribe_closed_group(args)
        test_closed_group_in_cache = database_helper.closed_group_cache.get(TEST_CLOSED_GROUP_ID)
        self.assertTrue(TEST_SESSION_ID in test_closed_group_in_cache.members)

    def test_6_unsubscribe_closed_group(self):
        args = {CLOSED_GROUP: TEST_CLOSED_GROUP_ID,
                PUBKEY: TEST_SESSION_ID}
        unsubscribe_closed_group(args)
        test_closed_group_in_cache = database_helper.closed_group_cache.get(TEST_CLOSED_GROUP_ID)
        self.assertFalse(TEST_SESSION_ID in test_closed_group_in_cache.members)


if __name__ == '__main__':
    unittest.main()
