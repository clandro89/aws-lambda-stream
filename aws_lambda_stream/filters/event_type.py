import re


def filter_on_event_type(rule, uow):
    # pylint: disable=no-else-return
    if isinstance(rule['event_type'], str):
        return uow['event']['type'] == rule['event_type']
    elif isinstance(rule['event_type'], list):
        return uow['event']['type'] in rule['event_type']
    elif callable(rule['event_type']):
        return rule['event_type'](uow)
    elif isinstance(rule['event_type'], re.Pattern):
        return bool(re.search(rule['event_type'], uow['event']['type']))
    else:
        raise Exception("Rule: {}: Must be a string, array of string, regex or function.".format(
            rule['id']
        ))
