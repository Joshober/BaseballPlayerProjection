import os

import pytest


@pytest.fixture(autouse=True)
def _dev_auth():
    os.environ.setdefault("SCOUTPRO_DEV_AUTH", "1")
