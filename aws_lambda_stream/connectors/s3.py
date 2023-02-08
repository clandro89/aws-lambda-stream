import boto3


class Connector():
    def __init__(self, bucket_name) -> None:
        self.bucket_name = bucket_name
        self.client = boto3.client('s3')
    def put_object(self, input_params):
        params = {
            'Bucket': self.bucket_name,
            **input_params
        }
        return self.client.put_object(**params)
    def get_object(self, input_params):
        params = {
            'Bucket': self.bucket_name,
            **input_params
        }
        return self.client.get_object(**params)
    def list_objects(self, input_params):
        params = {
            'Bucket': self.bucket_name,
            **input_params
        }
        return self.client.list_objects_v2(**params)
    def delete_object(self, input_params):
        params = {
            'Bucket': self.bucket_name,
            **input_params
        }
        return self.client.delete_object(**params)
