import os
import boto3
import requests
from requests_aws4auth import AWS4Auth
from pydash import omit_by

class Connector():
    def __init__(self, host, region = os.getenv('REGION')) -> None:
        self.host = host
        service = 'es'
        credentials = boto3.Session().get_credentials()
        self.awsauth = AWS4Auth(credentials.access_key,
                           credentials.secret_key,
                           region,
                           service,
                           session_token=credentials.token)
        self.session = requests.Session()
        self.session.hooks = {
            'response': lambda r, *args, **kwargs: r.raise_for_status()
        }
    def create_index(self, index, options):
        r = self.session.put(
            "{}/{}".format(self.host, index),
            auth=self.awsauth,
            headers={ "Content-Type":  "application/json" },
            json=options if options else {}
        )
        return r.json()
    def get_index(self, index):
        r = self.session.get(
            "{}/{}".format(self.host, index),
            auth=self.awsauth,
            headers={ "Content-Type":  "application/json" }
        )
        return r.json()
    def update_mapping(self, index, params):
        r = self.session.put(
            "{}/{}/_mapping".format(self.host, index),
            auth=self.awsauth,
            headers={ "Content-Type":  "application/json" },
            json=params
        )
        return r.json()
    def delete_index(self, index):
        r = self.session.delete(
            "{}/{}".format(self.host, index),
            auth=self.awsauth,
            headers={ "Content-Type":  "application/json" }
        )
        return r.json()
    def reindex(self, params):
        r = self.session.post(
            "{}/_reindex".format(self.host),
            auth=self.awsauth,
            headers={ "Content-Type":  "application/json" },
            json=params
        )
        return r.json()
    def create_or_update_alias(self, index, alias, params):
        r = self.session.put(
            "{}/{}/_alias/{}".format(self.host, index, alias),
            auth=self.awsauth,
            headers={ "Content-Type":  "application/json" },
            json=params if params else {}
        )
        return r.json()
    def search(self, index, payload, query=None):
        url = "{}/{}/_search".format(self.host, index)
        headers = { "Content-Type": "application/json" }
        payload = omit_by(
                payload,
                lambda value: value is None
        )
        r = requests.post(
            url,
            json=payload,
            params=query if query else {},
            auth=self.awsauth,
            headers=headers
        )
        return r.json()
    def put(self, index, _id, document):
        r = self.session.put(
            "{}/{}/_doc/{}".format(self.host, index, _id),
            auth=self.awsauth,
            json=document,
            headers={ "Content-Type": "application/json" }
        )
        return r.json()
    def delete(self, index, _id):
        r = self.session.delete(
            "{}/{}/_doc/{}".format(self.host, index, _id),
            auth=self.awsauth
        )
        return r.json()
    def bulk(self, params):
        r = self.session.post(
            "{}/_bulk".format(self.host),
            auth=self.awsauth,
            headers={ "Content-Type": "application/x-ndjson" },
            data=params
        )
        return r.json()
