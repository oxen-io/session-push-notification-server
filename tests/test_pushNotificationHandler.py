import unittest
from pushNotificationHandler import *
from lokiLogger import LokiLogger
from observer import Observer
from test_const import *

logger = LokiLogger().logger
observer = Observer(logger)
database_helper = DatabaseHelper(TEST_DATABASE)
database_helper.migrate_database_if_needed()
database_helper.load_cache()
PN_helper_v2 = PushNotificationHelperV2(logger, database_helper, observer)


class PushNotificationHandlerTests(unittest.TestCase):
    def setUp(self):
        self.database_helper = database_helper
        self.PN_helper_v2 = PN_helper_v2

    def tearDown(self):
        self.database_helper.flush()

    def test_0_register(self):
        self.PN_helper_v2.register(TEST_TOKEN_0, TEST_SESSION_ID)
        test_device_in_cache = self.database_helper.get_device(TEST_SESSION_ID)
        self.assertTrue(test_device_in_cache is not None,
                        'Test device was not saved to cache!')
        self.assertEqual(self.PN_helper_v2.push_fails[TEST_TOKEN_0], 0,
                         'The failure number of test_token_0 should be 0!')
        self.assertTrue(TEST_TOKEN_0 in test_device_in_cache.tokens,
                        'Test token was not stored properly!')

        self.PN_helper_v2.register(TEST_TOKEN_1, TEST_SESSION_ID)
        test_device_in_cache = self.database_helper.get_device(TEST_SESSION_ID)
        self.assertTrue(TEST_TOKEN_1 in test_device_in_cache.tokens,
                        'Test token was not stored properly!')
        self.assertEqual(len(test_device_in_cache.tokens), 2,
                         'Test device should have 2 tokens now!')

        self.PN_helper_v2.push_fails[TEST_TOKEN_0] += 3
        self.PN_helper_v2.register(TEST_TOKEN_0, TEST_SESSION_ID)
        test_device_in_cache = self.database_helper.get_device(TEST_SESSION_ID)
        self.assertEqual(len(test_device_in_cache.tokens), 2,
                         'Test device should have 2 tokens now!')
        self.assertEqual(self.PN_helper_v2.push_fails[TEST_TOKEN_0], 0,
                         'The failure number of test_token_0 should be 0!')

    def test_1_unregister(self):
        test_session_id = self.PN_helper_v2.remove_device_token(TEST_TOKEN_1)
        test_device_in_cache = self.database_helper.get_device(TEST_SESSION_ID)
        self.assertEqual(test_session_id, TEST_SESSION_ID,
                         'Did NOT find the correct session id.')
        self.assertEqual(len(test_device_in_cache.tokens), 1,
                         'Test device should have 1 token now!')
        self.assertTrue(TEST_TOKEN_1 not in test_device_in_cache.tokens,
                        'Test token was NOT removed properly.')
        self.assertTrue(self.PN_helper_v2.push_fails.get(TEST_TOKEN_1) is None,
                        'Push failure counter was NOT deleted.')

    def test_2_subscribe_closed_group(self):
        self.PN_helper_v2.subscribe_closed_group(TEST_CLOSED_GROUP_ID, TEST_SESSION_ID)
        test_closed_group_in_cache = self.database_helper.get_closed_group(TEST_CLOSED_GROUP_ID)
        self.assertTrue(test_closed_group_in_cache is not None,
                        'Test closed group was NOT saved to cache!')
        self.assertTrue(TEST_SESSION_ID in test_closed_group_in_cache.members,
                        'Test member was NOT stored properly.')

        self.PN_helper_v2.subscribe_closed_group(TEST_CLOSED_GROUP_ID, TEST_SESSION_ID_1)
        test_closed_group_in_cache = self.database_helper.get_closed_group(TEST_CLOSED_GROUP_ID)
        self.assertTrue(TEST_SESSION_ID_1 in test_closed_group_in_cache.members,
                        'Test member was NOT stored properly.')
        self.assertEqual(len(test_closed_group_in_cache.members), 2,
                         'Test closed group should have 2 members now!')

    def test_3_unsubscribe_closed_group(self):
        self.PN_helper_v2.unsubscribe_closed_group(TEST_CLOSED_GROUP_ID, TEST_SESSION_ID_1)
        test_closed_group_in_cache = self.database_helper.get_closed_group(TEST_CLOSED_GROUP_ID)
        self.assertEqual(len(test_closed_group_in_cache.members), 1,
                         'Test closed group should have 1 member now!')
        self.assertTrue(TEST_SESSION_ID_1 not in test_closed_group_in_cache.members,
                        'Test closed group member was NOT removed properly.')

    def test_4_send_push_notification(self):
        test_message = {'send_to': TEST_SESSION_ID,
                        'data': TEST_DATA}
        self.PN_helper_v2.add_message_to_queue(test_message)
        self.assertTrue(self.PN_helper_v2.message_queue.not_empty,
                        'The test message was NOT added to the message queue properly!')

        self.PN_helper_v2.send_push_notification()
        self.assertEqual(self.PN_helper_v2.notification_counter_android, 1,
                         'The push notification from test message was NOT generated!')

        test_closed_group_message = {'send_to': TEST_CLOSED_GROUP_ID,
                                     'data': TEST_DATA}
        self.PN_helper_v2.add_message_to_queue(test_closed_group_message)
        self.PN_helper_v2.send_push_notification()
        self.assertEqual(self.PN_helper_v2.closed_group_messages, 1,
                         'The push notification from test message was NOT generated!')
        self.assertEqual(self.PN_helper_v2.notification_counter_android, 2,
                         'The push notification from test message was NOT generated!')

    def test_5_handle_push_fail(self):
        self.PN_helper_v2.register(TEST_TOKEN_0, TEST_SESSION_ID)
        self.PN_helper_v2.handle_fail_result(TEST_TOKEN_0, '')
        self.assertEqual(self.PN_helper_v2.push_fails[TEST_TOKEN_0], 1,
                         'The push failure counter was NOT working properly.')

        for i in range(5):
            self.PN_helper_v2.handle_fail_result(TEST_TOKEN_0, '')
        test_device_in_cache = self.database_helper.get_device(TEST_SESSION_ID)
        self.assertTrue(TEST_TOKEN_0 not in test_device_in_cache.tokens,
                        'The failed token was NOT removed properly!')


if __name__ == '__main__':
    unittest.main()
