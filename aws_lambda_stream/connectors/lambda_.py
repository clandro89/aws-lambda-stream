import boto3


class Connector():

    def __init__(self, client = None) -> None:
        self.client = client if client else boto3.client('lambda')

    def invoke(self, params):
        return self.client.invoke(**params)
