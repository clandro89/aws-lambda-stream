import os
from pydash import get

def out_source_is_self(uow):
    # Use this filter in listeners to not consume events my service just emitted
    return (
        not 'tags' in uow['event'] or
        uow['event']['tags']['source'] != os.getenv('SERVICE')
    )

def out_latched(uow):
    # Use this filter in triggers to not emit events in reaction to an update from a listener
    # listeners should set latched = true
    # commands/mutations should set latched = null
    return (
        # create & update latch
        (get(uow,'event.raw.new') and not get(uow, 'event.raw.new.latched'))
        # delete latch
        or (not get(uow,'event.raw.new') and get(uow,'event.raw.old')
            and not get(uow, 'event.raw.old.latched'))
        )
