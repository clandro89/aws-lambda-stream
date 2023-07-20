from unittest.mock import MagicMock
import boto3
import pytest


mp = pytest.MonkeyPatch()
mp.setenv('STAGE', 'test')
mp.setattr(boto3, 'resource', lambda *_: MagicMock())
mp.setattr(boto3, 'client', lambda *_: MagicMock())
