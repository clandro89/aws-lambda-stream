import os
from pydash import get


# -----------------------------
# Integration Testing Support
# -----------------------------
# when recording integration tests, events will be generated to the stream
# and we want to ignore these test events downstream

def skip_tag():
    return {
        'skip': bool(os.getenv('PYTEST_CURRENT_TEST'))
    }

# use this filter in your pipelines to ignore these test events
def out_skip(uow):
    return not get(uow,'event.tags') or not get(uow, 'event.tags.skip')
