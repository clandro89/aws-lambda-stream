import requests
from pydash import omit_by


class Connector():

    def __init__(self, host: str, region: str, aws_session) -> None:
        self.host = host
        self.region = region
        self.aws_session = aws_session

    @property
    def auth(self):
        if self.awsauth:
            return self.awsauth
        from requests_aws4auth import AWS4Auth
        credentials = self.aws_session.get_credentials()
        service = 'es'
        self.awsauth = AWS4Auth(credentials.access_key,
                           credentials.secret_key,
                           self.region,
                           service,
                           session_token=credentials.token
                        )
        return self.awsauth

    def create_index(self, index, options):
        r = requests.put(
            "{}/{}".format(self.host, index),
            auth=self.auth,
            headers={ "Content-Type":  "application/json" },
            json=options if options else {}
        )
        return r.json()

    def get_index(self, index):
        r = requests.get(
            "{}/{}".format(self.host, index),
            auth=self.auth,
            headers={ "Content-Type":  "application/json" }
        )
        return r.json()

    def update_mapping(self, index, params):
        r = requests.put(
            "{}/{}/_mapping".format(self.host, index),
            auth=self.auth,
            headers={ "Content-Type":  "application/json" },
            json=params
        )
        return r.json()

    def delete_index(self, index):
        r = requests.delete(
            "{}/{}".format(self.host, index),
            auth=self.auth,
            headers={ "Content-Type":  "application/json" }
        )
        return r.json()

    def reindex(self, params):
        r = requests.post(
            "{}/_reindex".format(self.host),
            auth=self.auth,
            headers={ "Content-Type":  "application/json" },
            json=params
        )
        return r.json()

    def create_or_update_alias(self, index, alias, params):
        r = requests.put(
            "{}/{}/_alias/{}".format(self.host, index, alias),
            auth=self.auth,
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
            auth=self.auth,
            headers=headers
        )
        return r.json()

    def put(self, index, _id, document):
        r = requests.put(
            "{}/{}/_doc/{}".format(self.host, index, _id),
            auth=self.auth,
            json=document,
            headers={ "Content-Type": "application/json" }
        )
        return r.json()

    def delete(self, index, _id):
        r = requests.delete(
            "{}/{}/_doc/{}".format(self.host, index, _id),
            auth=self.auth
        )
        return r.json()

    def bulk(self, params):
        r = requests.post(
            "{}/_bulk".format(self.host),
            auth=self.auth,
            headers={ "Content-Type": "application/x-ndjson" },
            data=params
        )
        return r.json()
