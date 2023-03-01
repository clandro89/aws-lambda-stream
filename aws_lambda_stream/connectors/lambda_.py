import boto3


class Connector():
    def __init__(self) -> None:
        self.client = boto3.client('lambda')

    def invoke(self, params):
        return self.client.invoke(**params)
