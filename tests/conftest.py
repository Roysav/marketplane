import os

import pytest


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    dsn = os.environ.get("POSTGRES_DSN")
    if not dsn:
        pytest.skip("POSTGRES_DSN is not set")
    return dsn


@pytest.fixture(scope="session")
def redis_url() -> str:
    url = os.environ.get("REDIS_URL")
    if not url:
        pytest.skip("REDIS_URL is not set")
    return url
