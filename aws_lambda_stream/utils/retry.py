import os
import json
from time import sleep
from random import random

DEFAULT_RETRY_CONFIG = {
    'max_retries': os.getenv('BATCH_MAX_RETRIES') or 8,
    'retry_wait': os.getenv('BATCH_RETRY_WAIT') or 100
}

def assert_max_retries(attemps, max_retries):
    if len(attemps) > max_retries:
        raise Exception('Failed batch requests: {}'.format(
            json.dumps(attemps[-1])
        ))


def wait(ms):
    sleep(ms/1000)


def get_delay(base_millis, attempt):
    if not attempt:
        return 0

    exp_backoff = 2 ** attempt
    final_backoff  = exp_backoff + round(random() * base_millis)
    return base_millis + final_backoff
