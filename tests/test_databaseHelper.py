import json
import shutil
import unittest
from databaseHelper import *
from test_const import *
from server import database_helper

tests_cases = ['migrate_database_if_needed',
               'load_cache',
               'flush',
               'statistics_data']


class DatabaseHelperTests(unittest.TestCase):
    def setUp(self):
        self.databaseHelper = database_helper

    def tearDown(self):
        pass

    def test_0_migrate_database_if_needed(self):
        shutil.copyfile(f'../old db backup/{PUBKEY_TOKEN_DB_V2}', f'../tests/{PUBKEY_TOKEN_DB_V2}')
        shutil.copyfile(f'../old db backup/{CLOSED_GROUP_DB}', f'../tests/{CLOSED_GROUP_DB}')
        self.databaseHelper.migrate_database_if_needed()

        is_test_db_existed = os.path.isfile(DATABASE)
        self.assertTrue(is_test_db_existed)

        with open(DATABASE, 'rb') as test_db:
            db_map = dict(json.load(test_db))
        test_db.close()
        self.assertGreater(len(db_map.items()), 0)

        is_old_db_existed = os.path.isfile(PUBKEY_TOKEN_DB_V2) or os.path.isfile(CLOSED_GROUP_DB)
        self.assertFalse(is_old_db_existed)

    def test_1_load_cache(self):
        self.databaseHelper.load_cache()

        self.assertGreater(len(self.databaseHelper.device_cache.items()), 0)
        self.assertGreater(len(self.databaseHelper.token_device_mapping.items()), 0)
        self.assertGreater(len(self.databaseHelper.closed_group_cache.items()), 0)

    def test_2_flush(self):
        test_device = Device()
        test_device.session_id = TEST_SESSION_ID
        test_device.tokens.add(TEST_TOKEN_0)
        test_device.save(self.databaseHelper)

        test_device_in_cache = self.databaseHelper.get_device(TEST_SESSION_ID)
        self.assertFalse(test_device_in_cache is None)

        test_closed_group = ClosedGroup()
        test_closed_group.closed_group_id = TEST_CLOSED_GROUP_ID
        test_closed_group.members.add(TEST_SESSION_ID)
        test_closed_group.save(self.databaseHelper)

        test_closed_group_in_cache = self.databaseHelper.get_closed_group(TEST_CLOSED_GROUP_ID)
        self.assertFalse(test_closed_group_in_cache is None)

        self.databaseHelper.flush()
        self.databaseHelper.device_cache.clear()
        self.databaseHelper.closed_group_cache.clear()
        self.databaseHelper.load_cache()

        test_device_in_db = self.databaseHelper.get_device(TEST_SESSION_ID)
        self.assertFalse(test_device_in_db is None)

        test_closed_group_in_db = self.databaseHelper.get_closed_group(TEST_CLOSED_GROUP_ID)
        self.assertFalse(test_closed_group_in_db is None)

    def test_3_statistics_data(self):
        last_statistics_date = datetime.now()
        now = datetime.now()
        ios_pn_number = 1
        android_pn_number = 1
        total_message_number = 1
        closed_group_message_number = 1

        statistics_data = self.databaseHelper.get_data(None, None)
        total_columns_before = len(statistics_data[DATA])
        self.databaseHelper.store_data(last_statistics_date, now, ios_pn_number, android_pn_number, total_message_number, closed_group_message_number)
        statistics_data = self.databaseHelper.get_data(None, None)
        total_columns_after = len(statistics_data[DATA])

        self.assertEqual(total_columns_before + 1, total_columns_after)

        os.remove(DATABASE)


if __name__ == '__main__':
    unittest.main()





