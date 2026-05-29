from expects import equal, expect
from aws_lambda_stream.utils.decorators import reject_exception


def test_reject_exception_returns_exception_without_calling_function():
    calls = []
    err = Exception('boom')

    result = reject_exception(lambda value: calls.append(value))(err)

    expect(result).to(equal(err))
    expect(calls).to(equal([]))


def test_reject_exception_calls_function_for_non_exception_values():
    result = reject_exception(lambda value: value + 1)(1)

    expect(result).to(equal(2))
