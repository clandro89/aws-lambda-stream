

class Connector():

    def __init__(self, client = None) -> None:
        self._client = client

    @property
    def client(self):
        if not self._client:
            import boto3
            self._client = boto3.client('events')
        return self._client

    def put_events(self, params):
        response = self.client.put_events(**params)
        return response
