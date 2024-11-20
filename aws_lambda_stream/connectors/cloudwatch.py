
class Connector():

    def __init__(self, client = None) -> None:
        self._client = client

    @property
    def client(self):
        if not self._client:
            import boto3
            self._client = boto3.client('cloudwatch')
        return self._client

    def put(self, input_params):
        return self.client.put_metric_data(**input_params)
