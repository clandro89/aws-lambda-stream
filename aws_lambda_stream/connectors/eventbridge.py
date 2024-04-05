import boto3


class Connector():

    def __init__(self, client = None) -> None:
        self.client = client if client else boto3.client('events')

    def put_events(self, params):
        response = self.client.put_events(**params)
        return response
