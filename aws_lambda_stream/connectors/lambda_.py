
class Connector():

    def __init__(self, client = None) -> None:
        self._client = client

    @property
    def client(self):
        if not self._client:
            import boto3
            self._client = boto3.client('lambda')
        return self._client

    def invoke(self, params):
        return self.client.invoke(**params)
