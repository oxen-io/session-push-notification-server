import time


def process_expiration(expiration):
    # The expiration of a friend request message can be 4 days,
    # this method will process the expiration for friend request messages,
    # to make it like the expiration is one day.
    current_time = int(round(time.time() * 1000))
    ms_of_a_day = 24 * 60 * 60 * 1000
    if expiration - current_time > 3 * ms_of_a_day:
        expiration -= 3 * ms_of_a_day
    elif expiration - current_time > ms_of_a_day:
        expiration -= ms_of_a_day
    return expiration

def should_notify_for_message(abs_expiration):
    now = int(round(time.time() * 1000))
    day_in_ms = 24 * 60 * 60 * 1000
    max_delta_in_ms = 30 * 60 * 1000
    expiration = abs_expiration - now
    return expiration in range(day_in_ms - max_delta_in_ms, day_in_ms) or expiration in range(2 * day_in_ms - max_delta_in_ms, 2 * day_in_ms) \
            or expiration in range(4 * day_in_ms - max_delta_in_ms, 4 * day_in_ms)

def is_iOS_device_token(token):
    if len(token) == 64:
        return True
    return False
