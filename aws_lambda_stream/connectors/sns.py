import os
import boto3
from aws_lambda_stream.utils.aws import get_region

class Connector():
    def __init__(self,topic_arn = os.getenv('TOPIC_ARN')) -> None:
        self.topic_arn = topic_arn
        self.client = boto3.client('sns',
                                   region_name=get_region(topic_arn))

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
