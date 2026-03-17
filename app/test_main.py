import pytest
from fastapi.testclient import TestClient

from main import app


@pytest.fixture
def client():
    return TestClient(app)


def test_root(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "DevOps" in response.json()["message"]


def test_hello(client):
    response = client.get("/hello/World")
    assert response.status_code == 200
    assert response.json()["message"] == "Hello, World!"


def test_hello_custom_name(client):
    response = client.get("/hello/BTS")
    assert response.status_code == 200
    assert response.json()["message"] == "Hello, BTS!"


def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "version" in data
