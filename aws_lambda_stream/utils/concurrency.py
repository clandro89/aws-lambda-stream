import multiprocessing
from reactivex.scheduler import ThreadPoolScheduler


OPTIMAL_THREAD_COUNT = multiprocessing.cpu_count()

def get_pool_scheduler(threads=OPTIMAL_THREAD_COUNT)->ThreadPoolScheduler:
    return ThreadPoolScheduler(threads)
