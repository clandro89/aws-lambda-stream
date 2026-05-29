from array import array
from collections.abc import Mapping
from pydash import clone_deep_with, get


def _byte_length(value):
    if isinstance(value, memoryview):
        return value.nbytes
    if isinstance(value, array):
        return len(value) * value.itemsize
    return len(value)


def _eem_fields(uow):
    eem = get(uow, 'event.eem') or get(uow, 'undecryptedEvent.eem') or {}
    return get(eem, 'fields') or []


def trim_and_redact(_uow):
    fields = [
        field
        for uow in get(_uow, 'batch') or [_uow]
        for field in _eem_fields(uow)
    ]
    cache = []

    def clone_value(value, key=None):
        return clone_deep_with(value, lambda v, k=None, o=None: clone_customizer(v, key if key is not None else k, o))

    def clone_customizer(value, key=None, obj=None):  # pylint: disable=unused-argument
        if key in fields:
            return '[REDACTED]'

        if isinstance(value, (bytes, bytearray)):
            return '[BUFFER: {}]'.format(len(value))

        if isinstance(value, (memoryview, array)):
            return '[TYPED_ARRAY: {}]'.format(_byte_length(value))

        if isinstance(value, Mapping):
            if any(cached is value for cached in cache):
                return '[CIRCULAR]'
            cache.append(value)
            return {
                child_key: clone_value(child_value, child_key)
                for child_key, child_value in value.items()
            }

        if isinstance(value, list):
            if any(cached is value for cached in cache):
                return '[CIRCULAR]'
            cache.append(value)
            return [clone_value(child_value) for child_value in value]

        if isinstance(value, tuple):
            if any(cached is value for cached in cache):
                return '[CIRCULAR]'
            cache.append(value)
            return tuple(clone_value(child_value) for child_value in value)

        if isinstance(value, set):
            if any(cached is value for cached in cache):
                return '[CIRCULAR]'
            cache.append(value)
            return {clone_value(child_value) for child_value in value}

        return None

    def tr(uow):
        event = get(uow, 'undecryptedEvent') or get(uow, 'event')
        body = {
            key: value
            for key, value in uow.items()
            if key not in [
                'pipeline',
                'record',
                'event',
                'decryptResponse',
                'undecryptedEvent',
                'encryptResponse',
                'debug',
                'logger',
            ]
        }

        return {
            'pipeline': get(uow, 'pipeline'),
            'record': get(uow, 'record'),
            **clone_deep_with(
                {
                    'event': event,
                    **body,
                },
                clone_customizer,
            ),
        }

    if 'batch' not in _uow:
        return tr(_uow)

    body = {
        key: value
        for key, value in _uow.items()
        if key != 'batch'
    }

    return {
        'batch': [tr(uow) for uow in get(_uow, 'batch', [])],
        **clone_deep_with(body, clone_customizer),
    }
