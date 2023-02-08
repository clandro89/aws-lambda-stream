import os
import boto3


class Connector():
    def __init__(self,topic_arn = os.getenv('TOPIC_ARN')) -> None:
        self.topic_arn = topic_arn
        self.client = boto3.client('sns')
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
