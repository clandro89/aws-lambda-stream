from threading import current_thread
from reactivex import of
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.operators import rx_map

def map_except(_except):
    def wrapper(i):
        if i['key'] == _except:
            raise Exception(f"The value {_except} is not allowed")
        return i
    return wrapper


def test_rx_map():
    print("----test_rx_map-----")
    source = of({'key': "Alpha"},
                {'key':"Beta"},
                {'key': "Gamma"},
                {'key': "Delta"},
                {'key': 'Epsilon'}
            )

    composed = source.pipe(
        rx_map(lambda x: x),
        rx_map(lambda x: x),
        rx_map(faulty(map_except('Beta'))),
    )
    composed.subscribe(
        on_next=lambda i: print("PROCESS 2: {0} {1}".format(current_thread().name, i)),
        on_completed=lambda: print("Task 2 complete")
    )
