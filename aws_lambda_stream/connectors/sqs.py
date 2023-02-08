import os
import boto3


class Connector():
    def __init__(self,queue_url = os.getenv('QUEUE_URL')) -> None:
        self.queue_url = queue_url
        self.client = boto3.client('sqs')
    def send_message(self, input_params):
        params = {
            'QueueUrl': self.queue_url,
            **input_params
        }

        self.client.send_message(**params)
    def send_message_batch(self, input_params):
        params = {
            'QueueUrl': self.queue_url,
            **input_params
        }
        self.client.send_message_batch(**params)
