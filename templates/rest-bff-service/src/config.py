import os


REGION = os.getenv('REGION')
ENTITY_TABLE_NAME = os.getenv('ENTITY_TABLE_NAME')
ES_DOMAIN_HOST = f"https://{os.getenv('ES_DOMAIN_HOST')}"
