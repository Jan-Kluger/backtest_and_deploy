"""Pytest configuration and fixtures."""

import pytest


@pytest.fixture
def sample_db_config():
    """Sample database configuration for testing."""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "ctrade_test",
        "user": "postgres",
        "password": "",
    }

