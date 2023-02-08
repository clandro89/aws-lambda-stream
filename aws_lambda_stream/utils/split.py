from reactivex import Observable, operators as ops, from_list
from pydash import get
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.operators import rx_map


def split_object(rule):
    split_on = rule.get('split_on')
    split_target_field = rule.get('split_target_field', 'split')
    def wrapper(source: Observable):

        if split_on:
            if callable(split_on):
                _split = lambda uow: {'aux': split_on(uow, rule)}
            else:
                _split = lambda uow: {
                    'aux': list(
                        map(
                            lambda v: {
                                **uow,
                                split_target_field: v
                            },
                            get(uow, split_on, [])
                        )
                    ),
                }

            return source.pipe(
                rx_map(faulty(_split)),
                ops.flat_map(lambda uow: from_list(uow['aux']))# pylint: disable=E1102
            )

        return source
    return wrapper
