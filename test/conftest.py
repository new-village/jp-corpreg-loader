"""Shared pytest fixtures for the test suite."""
import json
import pytest
import jpcorpreg


@pytest.fixture(scope="session")
def expected_columns():
    """Expected CSV header columns from config."""
    with open("jpcorpreg/config/header.json", "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def shimane_df():
    """Live download of Shimane data as DataFrame (session-scoped)."""
    return jpcorpreg.load(prefecture="Shimane")
