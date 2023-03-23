import boto3

class Connector():
    def __init__(self) -> None:
        self.client = boto3.client('cloudwatch')

    def put(self, input_params):
        return self.client.put_metric_data(**input_params)
