"""Shared pytest fixtures for the test suite."""
import json
import pytest
from jpcorpreg.client import CorporateRegistryClient

@pytest.fixture(scope="session")
def expected_columns():
    """Expected CSV header columns from config."""
    with open("jpcorpreg/config/header.json", "r", encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture(scope="session")
def client():
    """Returns a shared CorporateRegistryClient."""
    return CorporateRegistryClient()

@pytest.fixture(scope="session")
def shimane_df(client):
    """Live download of Shimane data as DataFrame (session-scoped)."""
    return client.download_prefecture(prefecture="Shimane", format="df")
