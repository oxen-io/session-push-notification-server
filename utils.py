import time


def process_expiration(expiration):
    current_time = int(round(time.time() * 1000))
    ms_of_a_day = 24 * 60 *60 * 1000
    while expiration - current_time > ms_of_a_day:
        expiration -= ms_of_a_day
    return expiration