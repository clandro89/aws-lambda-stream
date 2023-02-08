


def reject_exception(fn):
    def wrapper(value):
        if isinstance(value, Exception):
            return value
        return fn(value)
    return wrapper
