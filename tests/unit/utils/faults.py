from aws_lambda_stream.utils.faults import faulty

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
