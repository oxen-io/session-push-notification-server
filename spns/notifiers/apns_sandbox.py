#
# Apple Push Notification service (sandbox mode)
#
# See apns.py
#

from .apns import APNSHandler, run

if __name__ == "__main__":
    run(startup_delay=0, sandbox=True)
