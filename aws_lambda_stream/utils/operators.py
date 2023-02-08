from typing import Callable
from reactivex import create, Observable, operators as ops
from .faults import faults


def split_buffer():
    def _wrapper(source: Observable):
        def subscribe(observer, scheduler = None):
            def on_next(value):
                for i in value:
                    observer.on_next(i)
            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler=scheduler)
        return create(subscribe)
    return _wrapper


def tap(fn):
    def _map(i):
        fn(i)
        return i
    def _wrapper(source: Observable):
        return source.pipe(
            ops.map(_map)
        )
    return _wrapper


def rx_map(mapper: Callable):

    def map_(source: Observable):
        def subscribe(
            observer, scheduler
        ):
            def on_next(value):
                try:
                    result = mapper(value)
                except Exception as err:  # pylint: disable=broad-except
                    faults(err)
                else:
                    observer.on_next(result)

            return source.subscribe(
                on_next, observer.on_error, observer.on_completed, scheduler=scheduler
            )

        return Observable(subscribe)

    return map_

def rx_filter(predicate: Callable):

    def filter_(source: Observable):
        def subscribe(
            observer, scheduler
        ):
            def on_next(value):
                try:
                    should_run = predicate(value)
                except Exception as ex:  # pylint: disable=broad-except
                    faults(ex)
                    return

                if should_run:
                    observer.on_next(value)

            return source.subscribe(
                on_next, observer.on_error, observer.on_completed, scheduler=scheduler
            )

        return Observable(subscribe)

    return filter_
