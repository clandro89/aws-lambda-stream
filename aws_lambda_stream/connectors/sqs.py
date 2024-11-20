import os


class Connector():

    def __init__(self, queue_url = os.getenv('QUEUE_URL'), client = None) -> None:
        self.queue_url = queue_url
        self._client = client

    @property
    def client(self):
        if not self._client:
            import boto3
            self._client = boto3.client('sqs')
        return self._client

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
