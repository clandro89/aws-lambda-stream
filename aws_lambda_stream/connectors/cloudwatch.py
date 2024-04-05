import boto3

class Connector():

    def __init__(self, client = None) -> None:
        self.client = client if client else boto3.client('cloudwatch')

    def put(self, input_params):
        return self.client.put_metric_data(**input_params)
