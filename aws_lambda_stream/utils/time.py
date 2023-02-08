import os
import time


def now():
    return int(time.time() * 1000)

def ttl(start, days):
    return int(start/1000) + (60*60*24*days)

def ttl_rule(rule, uow):
    if callable(rule.get('ttl')):
        return rule['ttl'](rule, uow)
    return ttl(
        uow['event']['timestamp'],
        rule.get('ttl') or rule.get('default_ttl') or os.getenv('TTL') or 33
    )
