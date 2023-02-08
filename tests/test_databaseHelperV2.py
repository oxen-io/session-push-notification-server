import unittest
from model.databaseModelV2 import *
from tools.databaseHelperV2 import DatabaseHelperV2
from test_const import *
from model.pushNotificationStats import *


tests_cases = ['populate_cache',
               'flush',
               'statistics_data']


class DatabaseHelperV2Tests(unittest.TestCase):
    def setUp(self):
        self.databaseHelper = DatabaseHelperV2()

    def tearDown(self):
        pass

    def test_0_populate_cache(self):
        self.databaseHelper.populate_cache()

        self.assertGreater(len(self.databaseHelper.device_cache.items()), 0)
        self.assertGreater(len(self.databaseHelper.token_device_mapping.items()), 0)
        self.assertGreater(len(self.databaseHelper.closed_group_cache.items()), 0)

    def test_1_flush(self):
        test_device = Device()
        test_device.session_id = TEST_SESSION_ID
        test_device.add_token(Device.Token(TEST_TOKEN_0, DeviceType.Unknown))
        test_device.save_to_cache(self.databaseHelper)

        test_device_in_cache = self.databaseHelper.get_device(TEST_SESSION_ID)
        self.assertFalse(test_device_in_cache is None)

        test_closed_group = ClosedGroup()
        test_closed_group.closed_group_id = TEST_CLOSED_GROUP_ID
        test_closed_group.add_member(TEST_SESSION_ID)
        test_closed_group.save_to_cache(self.databaseHelper)

        test_closed_group_in_cache = self.databaseHelper.get_closed_group(TEST_CLOSED_GROUP_ID)
        self.assertFalse(test_closed_group_in_cache is None)

        self.databaseHelper.flush()
        self.assertFalse(test_device_in_cache.needs_to_be_updated)
        self.assertFalse(test_closed_group_in_cache.needs_to_be_updated)

        self.databaseHelper.device_cache.clear()
        self.databaseHelper.closed_group_cache.clear()
        self.databaseHelper.populate_cache()

        test_device_in_db = self.databaseHelper.get_device(TEST_SESSION_ID)
        self.assertFalse(test_device_in_db is None)

        test_closed_group_in_db = self.databaseHelper.get_closed_group(TEST_CLOSED_GROUP_ID)
        self.assertFalse(test_closed_group_in_db is None)

    def test_2_statistics_data(self):
        stats_data = PushNotificationStats()
        stats_data.increment_ios_pn(1)
        stats_data.increment_android_pn(1)
        stats_data.increment_total_message(1)
        stats_data.increment_closed_group_message(1)
        stats_data.increment_untracked_message(1)
        stats_data.increment_deduplicated_one_on_one_message(1)

        statistics_data = self.databaseHelper.get_stats_data(None, None)
        total_columns_before = len(statistics_data[PushNotificationStats.ResponseKey.DATA])
        self.databaseHelper.store_stats_data(stats_data)
        statistics_data = self.databaseHelper.get_stats_data(None, None)
        total_columns_after = len(statistics_data[PushNotificationStats.ResponseKey.DATA])

        self.assertEqual(total_columns_before + 1, total_columns_after)

    def test_3_adding_duplicated_token(self):
        test_device = Device()
        test_device.session_id = TEST_SESSION_ID
        test_device.add_token(Device.Token(TEST_TOKEN_0, DeviceType.Unknown))
        test_device.save_to_cache(self.databaseHelper)
        self.databaseHelper.flush()

        test_device_in_cache = self.databaseHelper.get_device(TEST_SESSION_ID)
        test_device_in_cache.add_token(Device.Token(TEST_TOKEN_0, DeviceType.Unknown))
        self.assertFalse(test_device_in_cache.needs_to_be_updated)


if __name__ == '__main__':
    unittest.main()
