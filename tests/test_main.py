import unittest
from test_databaseHelperV2 import tests_cases as database_helper_test_cases
from test_databaseHelperV2 import DatabaseHelperV2Tests
from test_pushNotificationHandler import tests_cases as push_notification_handler_test_cases
from test_pushNotificationHandler import PushNotificationHandlerTests
from test_server import tests_cases as server_test_cases
from test_server import ServerTests


def suite():
    test_suite = unittest.TestSuite()

    index = 0
    for test_case in database_helper_test_cases:
        test_suite.addTest(DatabaseHelperV2Tests(f'test_{index}_{test_case}'))
        index += 1

    index = 0
    for test_case in push_notification_handler_test_cases:
        test_suite.addTest(PushNotificationHandlerTests(f'test_{index}_{test_case}'))
        index += 1

    index = 0
    for test_case in server_test_cases:
        test_suite.addTest(ServerTests(f'test_{index}_{test_case}'))
        index += 1

    return test_suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())
