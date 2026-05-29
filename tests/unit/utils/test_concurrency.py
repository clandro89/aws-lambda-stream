from expects import equal, expect
from reactivex import from_list
from reactivex.scheduler import ThreadPoolScheduler
from aws_lambda_stream.utils.concurrency import get_pool_scheduler, multitask


def test_get_pool_scheduler():
    scheduler = get_pool_scheduler(1)

    expect(isinstance(scheduler, ThreadPoolScheduler)).to(equal(True))
    scheduler.executor.shutdown()


def test_multitask():
    collected = []

    from_list([ # pylint: disable=E1102
        {
            'tasks': [
                {
                    'task': lambda value: value + 1,
                    'params': [1],
                },
                {
                    'task': lambda value: value * 2,
                    'params': [3],
                },
            ],
        },
    ]).pipe(
        multitask(),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect(collected[0]['result']).to(equal([2, 6]))


def test_multitask_returns_exceptions_as_results():
    collected = []

    def fail():
        raise Exception('boom')

    from_list([ # pylint: disable=E1102
        {
            'tasks': [
                {
                    'task': fail,
                    'params': [],
                },
            ],
        },
    ]).pipe(
        multitask(),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect(isinstance(collected[0]['result'][0], Exception)).to(equal(True))
    expect(str(collected[0]['result'][0])).to(equal('boom'))
