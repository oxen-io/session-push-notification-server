import unittest
import asyncio
from test_const import *
from tools.databaseHelperV2 import DatabaseHelperV2
from model.databaseModelV2 import Device
from tools.pushNotificationHandler import PushNotificationHelperV2

tests_cases = ['register',
               'unregister',
               'subscribe_closed_group',
               'unsubscribe_closed_group',
               'send_push_notification',
               'handle_push_fail',
               'register_legacy_groups_only']


class PushNotificationHandlerTests(unittest.TestCase):
    def setUp(self):
        self.database_helper = DatabaseHelperV2()
        self.PN_helper_v2 = PushNotificationHelperV2()

    def tearDown(self):
        self.database_helper.flush()

    def test_0_register(self):
        self.PN_helper_v2.register(TEST_TOKEN_0, TEST_SESSION_ID, TEST_DEVICE_TYPE)
        test_device_in_cache = self.database_helper.get_device(TEST_SESSION_ID)
        self.assertFalse(test_device_in_cache is None)
        self.assertEqual(self.PN_helper_v2.push_fails[TEST_TOKEN_0], 0)
        self.assertTrue(Device.Token(TEST_TOKEN_0, TEST_DEVICE_TYPE) in test_device_in_cache.tokens)
        self.assertTrue(TEST_TOKEN_0 in self.database_helper.token_device_mapping.keys())
        self.assertEqual(self.database_helper.token_device_mapping[TEST_TOKEN_0], test_device_in_cache)

        self.PN_helper_v2.register(TEST_TOKEN_1, TEST_SESSION_ID, TEST_DEVICE_TYPE)
        test_device_in_cache = self.database_helper.get_device(TEST_SESSION_ID)
        self.assertTrue(Device.Token(TEST_TOKEN_1, TEST_DEVICE_TYPE) in test_device_in_cache.tokens)
        self.assertEqual(len(test_device_in_cache.tokens), 2)
        self.assertTrue(TEST_TOKEN_1 in self.database_helper.token_device_mapping.keys())
        self.assertEqual(self.database_helper.token_device_mapping[TEST_TOKEN_1], test_device_in_cache)

        self.PN_helper_v2.push_fails[TEST_TOKEN_0] += 3
        self.PN_helper_v2.register(TEST_TOKEN_0, TEST_SESSION_ID, TEST_DEVICE_TYPE)
        test_device_in_cache = self.database_helper.get_device(TEST_SESSION_ID)
        self.assertEqual(len(test_device_in_cache.tokens), 2)
        self.assertEqual(self.PN_helper_v2.push_fails[TEST_TOKEN_0], 0)

    def test_1_unregister(self):
        test_session_id = self.PN_helper_v2.remove_device_token(TEST_TOKEN_1)
        test_device_in_cache = self.database_helper.get_device(TEST_SESSION_ID)
        self.assertEqual(test_session_id, TEST_SESSION_ID)
        self.assertEqual(len(test_device_in_cache.tokens), 1)
        self.assertFalse(Device.Token(TEST_TOKEN_1, TEST_DEVICE_TYPE) in test_device_in_cache.tokens)
        self.assertTrue(self.PN_helper_v2.push_fails.get(TEST_TOKEN_1) is None)
        self.assertFalse(TEST_TOKEN_1 in self.database_helper.token_device_mapping.keys())
        self.assertTrue(self.database_helper.token_device_mapping.get(TEST_TOKEN_1) is None)

    def test_2_subscribe_closed_group(self):
        self.PN_helper_v2.subscribe_closed_group(TEST_CLOSED_GROUP_ID, TEST_SESSION_ID)
        test_closed_group_in_cache = self.database_helper.get_closed_group(TEST_CLOSED_GROUP_ID)
        self.assertFalse(test_closed_group_in_cache is None)
        self.assertTrue(TEST_SESSION_ID in test_closed_group_in_cache.members)

        self.PN_helper_v2.subscribe_closed_group(TEST_CLOSED_GROUP_ID, TEST_SESSION_ID_1)
        test_closed_group_in_cache = self.database_helper.get_closed_group(TEST_CLOSED_GROUP_ID)
        self.assertTrue(TEST_SESSION_ID_1 in test_closed_group_in_cache.members)
        self.assertEqual(len(test_closed_group_in_cache.members), 2)

    def test_3_unsubscribe_closed_group(self):
        self.PN_helper_v2.unsubscribe_closed_group(TEST_CLOSED_GROUP_ID, TEST_SESSION_ID_1)
        test_closed_group_in_cache = self.database_helper.get_closed_group(TEST_CLOSED_GROUP_ID)
        self.assertEqual(len(test_closed_group_in_cache.members), 1)
        self.assertFalse(TEST_SESSION_ID_1 in test_closed_group_in_cache.members)

    def test_4_send_push_notification(self):
        test_message = {'send_to': TEST_SESSION_ID,
                        'data': TEST_DATA}
        self.PN_helper_v2.add_message_to_queue(test_message)
        self.assertTrue(self.PN_helper_v2.message_queue.not_empty)

        loop = asyncio.get_event_loop()
        coroutine = self.PN_helper_v2.send_push_notification()
        loop.run_until_complete(coroutine)
        self.assertEqual(self.PN_helper_v2.stats_data.notification_counter_android, 1)

        test_closed_group_message = {'send_to': TEST_CLOSED_GROUP_ID,
                                     'data': TEST_DATA}
        self.PN_helper_v2.add_message_to_queue(test_closed_group_message)
        loop = asyncio.get_event_loop()
        coroutine = self.PN_helper_v2.send_push_notification()
        loop.run_until_complete(coroutine)
        self.assertEqual(self.PN_helper_v2.stats_data.closed_group_messages, 1)
        self.assertEqual(self.PN_helper_v2.stats_data.notification_counter_android, 2)

    def test_5_handle_push_fail(self):
        self.PN_helper_v2.register(TEST_TOKEN_0, TEST_SESSION_ID, TEST_DEVICE_TYPE)
        self.PN_helper_v2.handle_fail_result(TEST_TOKEN_0, '')
        self.assertEqual(self.PN_helper_v2.push_fails[TEST_TOKEN_0], 1)

        for i in range(5):
            self.PN_helper_v2.handle_fail_result(TEST_TOKEN_0, '')
        test_device_in_cache = self.database_helper.get_device(TEST_SESSION_ID)
        self.assertFalse(Device.Token(TEST_TOKEN_0, TEST_DEVICE_TYPE) in test_device_in_cache.tokens)

    def test_6_register_legacy_groups_only(self):
        self.PN_helper_v2.register(TEST_TOKEN_0, TEST_SESSION_ID, TEST_DEVICE_TYPE)
        test_device_in_cache = self.database_helper.get_device(TEST_SESSION_ID)
        self.assertFalse(test_device_in_cache.legacy_groups_only)

        self.PN_helper_v2.register_legacy_groups_only(TEST_TOKEN_0, TEST_SESSION_ID, TEST_DEVICE_TYPE, [TEST_CLOSED_GROUP_ID])
        test_device_in_cache = self.database_helper.get_device(TEST_SESSION_ID)
        self.assertTrue(test_device_in_cache.legacy_groups_only)

        test_message = {'send_to': TEST_SESSION_ID,
                        'data': TEST_DATA}
        self.PN_helper_v2.add_message_to_queue(test_message)
        loop = asyncio.get_event_loop()
        coroutine = self.PN_helper_v2.send_push_notification()
        loop.run_until_complete(coroutine)
        self.assertEqual(self.PN_helper_v2.stats_data.notification_counter_android, 0)

        test_closed_group_message = {'send_to': TEST_CLOSED_GROUP_ID,
                                     'data': TEST_DATA}
        self.PN_helper_v2.add_message_to_queue(test_closed_group_message)
        loop = asyncio.get_event_loop()
        coroutine = self.PN_helper_v2.send_push_notification()
        loop.run_until_complete(coroutine)
        self.assertEqual(self.PN_helper_v2.stats_data.closed_group_messages, 1)
        self.assertEqual(self.PN_helper_v2.stats_data.notification_counter_android, 1)


if __name__ == '__main__':
    unittest.main()
