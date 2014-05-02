"""
The ``neodb.keygen`` module contains functions for generating unique keys of various lengths.
"""

import random

def gen_key(size=11, alphabet='23456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ'):
    return ''.join(random.choice(alphabet) for _ in xrange(size))

def gen_short_key():
    """
    Generates a short key (11 chars) with a high probability of uniqueness
    """
    return gen_key(size=11)

def gen_medium_key():
    """
    Generates a medium key (22 chars) with a higher probability of uniqueness
    """
    return gen_key(size=22)

def gen_long_key():
    """
    Generates a long key (44 chars) with the highest probability of uniqueness
    """
    return gen_key(size=44)

generate_key = gen_medium_key
