import json
import shutil
import unittest
from databaseHelper import *

TEST_DATABASE = "test_pn_server_db.json"


class DatabaseHelperTests(unittest.TestCase):
    def setUp(self):
        self.databaseHelper = DatabaseHelper(TEST_DATABASE)

    def tearDown(self):
        self.databaseHelper.tinyDB.close()

    def test_0_migrate_database_if_needed(self):
        shutil.copyfile(f'../old db backup/{PUBKEY_TOKEN_DB_V2}', f'../tests/{PUBKEY_TOKEN_DB_V2}')
        shutil.copyfile(f'../old db backup/{CLOSED_GROUP_DB}', f'../tests/{CLOSED_GROUP_DB}')
        self.databaseHelper.migrate_database_if_needed()

        is_test_db_existed = os.path.isfile(TEST_DATABASE)
        self.assertTrue(is_test_db_existed,
                        'The test database file should be existed!')

        with open(TEST_DATABASE, 'rb') as test_db:
            db_map = dict(json.load(test_db))
        test_db.close()
        self.assertTrue(len(db_map.items()) > 0,
                        'The test database should not be empty!')

        is_old_db_existed = os.path.isfile(PUBKEY_TOKEN_DB_V2) or os.path.isfile(CLOSED_GROUP_DB)
        self.assertTrue(not is_old_db_existed,
                        'The old database file should not be existed!')

    def test_1_load_cache(self):
        self.databaseHelper.load_cache()

        self.assertTrue(len(self.databaseHelper.device_cache.items()) > 0,
                        'The device cache was not loaded!')
        self.assertTrue(len(self.databaseHelper.closed_group_cache.items()) > 0,
                        'The closed group cache was not loaded!')

    def test_2_flush(self):
        test_device = Device()
        test_device.session_id = 'test_session_id'
        test_device.tokens.add('test_token')
        test_device.save(self.databaseHelper)

        test_device_in_cache = self.databaseHelper.get_device('test_session_id')
        self.assertTrue(test_device_in_cache is not None,
                        'Test device was not saved to cache!')

        test_closed_group = ClosedGroup()
        test_closed_group.closed_group_id = 'test_closed_group_id'
        test_closed_group.members.add('test_session_id')
        test_closed_group.save(self.databaseHelper)

        test_closed_group_in_cache = self.databaseHelper.get_closed_group('test_closed_group_id')
        self.assertTrue(test_closed_group_in_cache is not None,
                        'Test closed group was not saved to cache!')

        self.databaseHelper.flush()
        self.databaseHelper.device_cache.clear()
        self.databaseHelper.closed_group_cache.clear()
        self.databaseHelper.load_cache()

        test_device_in_db = self.databaseHelper.get_device('test_session_id')
        self.assertTrue(test_device_in_db is not None,
                        'Test device was not flushed to database!')

        test_closed_group_in_db = self.databaseHelper.get_closed_group('test_closed_group_id')
        self.assertTrue(test_closed_group_in_db is not None,
                        'Test closed group was not flushed to database!')

    def test_3_statistics_data(self):
        last_statistics_date = datetime.now()
        now = datetime.now()
        ios_pn_number = 1
        android_pn_number = 1
        total_message_number = 1
        closed_group_message_number = 1

        statistics_data = self.databaseHelper.get_data(None, None)
        total_columns_before = len(statistics_data)
        self.databaseHelper.store_data(last_statistics_date, now, ios_pn_number, android_pn_number, total_message_number, closed_group_message_number)
        statistics_data = self.databaseHelper.get_data(None, None)
        total_columns_after = len(statistics_data)

        self.assertEqual(total_columns_before + 1, total_columns_after,
                         'The statistics data was not inserted into the database!')

        os.remove(TEST_DATABASE)


if __name__ == '__main__':
    unittest.main()





