from random import random
import time
from contextlib import contextmanager
from google.appengine.api import memcache

class LockUnavailable(Exception):

    """
    A lock on the specified mutex could not be acquired within the specified time or retry constraints.
    """
    def __init__(self, mutex):
        self.mutex = mutex

    def __str__(self):
        'Unable to lock on: {0} within the specified time or retry constraints.'.format(self.mutex)


@contextmanager
def lock(mutex, expiry=10*60, retries=5, backoff=2):
    for retry in xrange(retries):
        if memcache.add(mutex, True, time=expiry, namespace='mutex'): break
        time.sleep(backoff ** retry + random() * 1)
    else:
        raise LockUnavailable(mutex)
    try:
        yield
    finally:
        memcache.delete(mutex, namespace='mutex')



