
class Connector():

    def __init__(self, bucket_name, client = None) -> None:
        self.bucket_name = bucket_name
        self._client = client

    @property
    def client(self):
        if not self._client:
            import boto3
            self._client = boto3.client('s3')
        return self._client

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
