from expects import equal, expect
from aws_lambda_stream.utils import faults as faults_module
from aws_lambda_stream.utils.faults import faulty, flush_faults, format_fault, faults

# pylint: disable=broad-except
def test_faulty_no_ignore_exception():
    def do_exception(uow):
        raise Exception('Fail')
    uow = {
        'id':'EID',
        'type': 'thing-created'
    }
    try:
        faulty(do_exception)(uow)
        assert False
    except Exception as err:
        assert hasattr(err, 'uow')

def test_faulty_ignore_exception():
    def do_exception(uow):
        raise Exception('Fail')
    uow = {
        'id':'EID',
        'type': 'thing-created'
    }
    try:
        faulty(do_exception, True)(uow)
        assert False
    except Exception as err:
        assert not hasattr(err, 'uow')


def test_format_fault(monkeypatch):
    monkeypatch.setenv('AWS_LAMBDA_FUNCTION_NAME', 'function')
    err = Exception('boom')
    err.uow = {
        'pipeline': 'test',
        'event': {
            'id': 'evt-1',
        },
    }

    fault = format_fault(err)

    expect(fault['type']).to(equal('fault'))
    expect(fault['tags']).to(equal({
        'functionname': 'function',
        'pipeline': 'test',
    }))
    expect(fault['err']['name']).to(equal('Exception'))
    expect(fault['err']['message']).to(equal('boom'))
    expect(fault['uow']['event']).to(equal({'id': 'evt-1'}))


def test_faults_collects_faults(monkeypatch):
    monkeypatch.setattr(faults_module, 'the_faults', [])
    err = Exception('boom')
    err.uow = {
        'pipeline': 'test',
        'event': {},
    }

    faults(err)

    expect(len(faults_module.the_faults)).to(equal(1))
    expect(faults_module.the_faults[0]['err']['message']).to(equal('boom'))


def test_faults_raises_unhandled_errors():
    try:
        faults(Exception('boom'))
        assert False
    except Exception as err:  # pylint: disable=broad-except
        expect(str(err)).to(equal('boom'))


class Logger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)


def test_flush_faults(monkeypatch):
    logger = Logger()
    published = []
    monkeypatch.setattr(faults_module, 'the_faults', [
        {
            'id': 'fault-1',
            'type': 'fault',
        },
    ])

    def publish(_):
        def wrapper(source):
            def on_next(uow):
                published.append(uow)
            source.subscribe(on_next=on_next)
            return source
        return wrapper

    flush_faults({
        'logger': logger,
        'publish': publish,
    })

    expect(logger.messages[0]).to(equal('flush faults'))
    expect(published).to(equal([
        {
            'event': {
                'id': 'fault-1',
                'type': 'fault',
            },
        },
    ]))
    expect(faults_module.the_faults).to(equal([]))
