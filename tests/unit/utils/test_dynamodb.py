from expects import expect, have_keys
from aws_lambda_stream.utils.dynamodb import update_expression


def test_update_expression():
    item = {'pk': 'abc', 'st': 'thing', 'name': 'thing name','as':None}
    result = update_expression(item)
    expect(result).to(
        have_keys(
            'ExpressionAttributeNames',
            'ExpressionAttributeValues',
            'UpdateExpression'
            )
    )
