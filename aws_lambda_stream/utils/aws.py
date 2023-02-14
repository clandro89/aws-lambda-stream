import os


def get_region(arn: str):
    elements = arn.split(':', 5)
    return elements[3] if len(elements) > 5 else os.getenv('REGION')
