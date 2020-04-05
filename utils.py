import time


def process_expiration(expiration):
    # The expiration of a friend request message can be 4 days,
    # this method will process the expiration for friend request messages,
    # to make it like the expiration is one day.
    current_time = int(round(time.time() * 1000))
    ms_of_a_day = 24 * 60 * 60 * 1000
    if expiration - current_time > ms_of_a_day:
        expiration -= 3 * ms_of_a_day
    return expiration
