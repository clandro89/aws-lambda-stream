import os

from aws_lambda_stream.utils.aws import get_region

class Connector():
    def __init__(self, topic_arn = os.getenv('TOPIC_ARN'), client = None) -> None:
        self.topic_arn = topic_arn
        self._client = client

    @property
    def client(self):
        if not self._client:
            import boto3
            self._client = boto3.client('sns', region_name=get_region(self.topic_arn))
        return self._client

    def publish(self, input_params):
        params = {
            'TopicArn': self.topic_arn,
            **input_params
        }
        return self.client.publish(**params)

    def publish_batch(self, input_params):
        params = {
            'TopicArn': self.topic_arn,
            **input_params
        }
        return self.client.publish_batch(**params)
