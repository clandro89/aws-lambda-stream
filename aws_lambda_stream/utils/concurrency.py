import asyncio
from typing import Callable
import multiprocessing
from pydash import get, set_
from reactivex.scheduler import ThreadPoolScheduler
from reactivex import Observable
from .operators import rx_map
from .faults import faulty



OPTIMAL_THREAD_COUNT = multiprocessing.cpu_count()

def get_pool_scheduler(threads=OPTIMAL_THREAD_COUNT)->ThreadPoolScheduler:
    return ThreadPoolScheduler(threads)


async def _execute_task(
    task: Callable,
    *args
    ):
    def wrapper(*p):
        try:
            return task(*p)
        except Exception as e:
            return e

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, wrapper, *args)


def multitask(
    tasks_field = 'tasks',
    task_field = 'task',
    params_field = 'params',
    result_field = 'result'
    ):
    def wrapper(source: Observable):
        async def execute(uow):
            tasks = [ _execute_task(i[task_field], *i[params_field])
                     for i in get(uow, tasks_field)
                    ]
            return await asyncio.gather(*tasks)

        return source.pipe(
            rx_map(faulty(
                lambda uow: set_(
                    uow,
                    result_field,
                    asyncio.run(execute(uow))
                ),
            )),
        )
    return wrapper
