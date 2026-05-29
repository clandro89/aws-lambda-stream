from threading import current_thread
from expects import equal, expect
from reactivex import from_list, of
from aws_lambda_stream.utils import faults as faults_module
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.operators import rx_filter, rx_map, split_buffer, tap

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


def test_split_buffer():
    collected = []

    from_list([ # pylint: disable=E1102
        [1, 2],
        [3],
    ]).pipe(
        split_buffer(),
    ).subscribe(
        on_next=lambda value: collected.append(value)
    )

    expect(collected).to(equal([1, 2, 3]))


def test_tap_keeps_original_value():
    calls = []
    collected = []

    from_list([1]).pipe( # pylint: disable=E1102
        tap(lambda value: calls.append(value)),
    ).subscribe(
        on_next=lambda value: collected.append(value)
    )

    expect(calls).to(equal([1]))
    expect(collected).to(equal([1]))


def test_rx_filter():
    collected = []

    from_list([1, 2, 3]).pipe( # pylint: disable=E1102
        rx_filter(lambda value: value > 1),
    ).subscribe(
        on_next=lambda value: collected.append(value)
    )

    expect(collected).to(equal([2, 3]))


def test_rx_map_and_filter_forward_faults(monkeypatch):
    errors = []
    monkeypatch.setattr(faults_module, 'the_faults', [])

    from_list([ # pylint: disable=E1102
        {
            'pipeline': 'test',
            'event': {
                'id': 'evt-1',
            },
        },
    ]).pipe(
        rx_map(faulty(lambda _: (_ for _ in ()).throw(Exception('map failed')))),
        rx_filter(lambda _: (_ for _ in ()).throw(Exception('filter failed'))),
    ).subscribe(
        on_error=lambda error: errors.append(error)
    )

    expect(errors).to(equal([]))
    expect(len(faults_module.the_faults)).to(equal(1))
    expect(faults_module.the_faults[0]['err']['message']).to(equal('map failed'))
