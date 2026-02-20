"""
conftest.py — pytest fixtures shared across all test modules.
Uses a separate test schema to avoid touching the real pipeline data.
"""

import os
import pytest
import psycopg2

# Override DB settings for test isolation
TEST_DB = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "scd2db"),
    "user": os.getenv("POSTGRES_USER", "scd2user"),
    "password": os.getenv("POSTGRES_PASSWORD", "scd2pass"),
}


@pytest.fixture(scope="session")
def db_conn():
    """Session-scoped DB connection for integration tests."""
    conn = psycopg2.connect(**TEST_DB)
    conn.autocommit = False
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def rollback_after_test(db_conn):
    """Roll back every test transaction so tests don't pollute each other."""
    yield
    db_conn.rollback()
