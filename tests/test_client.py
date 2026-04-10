"""Tests for the Confluence client."""

import pytest


@pytest.fixture
def sample_fixture():
    """Sample test fixture."""
    return "test_data"


def test_placeholder(sample_fixture):
    """Placeholder test."""
    assert sample_fixture == "test_data"