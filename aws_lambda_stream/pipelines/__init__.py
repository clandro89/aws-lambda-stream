from functools import reduce
import multiprocessing
import copy
from reactivex import operators as ops, Observable, from_list
from reactivex.scheduler import ThreadPoolScheduler
from aws_lambda_powertools import Logger
from aws_lambda_stream.utils.operators import tap
from aws_lambda_stream.utils.faults import flush_faults


def initialize_from(rules: list):
    return reduce(lambda accumulator, rule: {
            **accumulator,
            rule['id']: lambda opt: rule['flavor']({
                **opt,
                **rule
            })
        }, rules, {})


class StreamPipeline():
    def __init__(self, pipelines: dict, opt: dict, concurrency = True) -> None:
        self.keys = pipelines.keys()
        self.the_pipelines = pipelines
        self.opt = opt
        self.subscriptions = []
        self.concurrency = concurrency

    def assemble(self,
                 iterable,
                 on_next=None,
                 on_error=None,
                 on_completed=None):
        # calculate number of CPUs, then create a ThreadPoolScheduler with that number of threads
        optimal_thread_count = multiprocessing.cpu_count()
        pool_scheduler = ThreadPoolScheduler(optimal_thread_count)

        def make_lines(k):
            p = from_list( # pylint: disable=E1102
                copy.deepcopy(iterable)
            ).pipe(
                ops.map(lambda uow: {
                    'pipeline': k,
                    **uow,
                }),
                self.the_pipelines[k](
                    {
                        'id': k,
                        'pipeline': copy.copy(k),
                        **self.opt,
                        'logger': Logger(k),
                        'max_batch_size': len(iterable)
                    }
                )
            )
            p.id = k
            return p

        lines = list(map(make_lines, self.the_pipelines.keys()))

        def _emit(source: Observable): #pylint: disable=no-self-use
            def _on_next(pipeline_id, uow):
                if on_next:
                    on_next(pipeline_id, uow)
            def _on_error(pipeline_id, err):
                if on_error:
                    on_error(pipeline_id, err)
            def _on_completed(pipeline_id):
                if on_completed:
                    on_completed(pipeline_id)

            source.subscribe(
                on_next=lambda i: _on_next(source.id, i),
                on_error=lambda e: _on_error(source.id, e),
                on_completed=lambda *_: _on_completed(source.id),
                **({'scheduler': pool_scheduler} if self.concurrency else {})
            )

        from_list(lines).pipe( # pylint: disable=E1102
            tap(_emit)
        ).subscribe()

        pool_scheduler.executor.shutdown()
        flush_faults({
            **self.opt
        })
