import boto3


class Connector():
    def __init__(self) -> None:
        self.client = boto3.client('events')
    def put_events(self, params):
        response = self.client.put_events(**params)
        return response
