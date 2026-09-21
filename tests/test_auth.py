"""
Auth endpoint tests.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

os.environ["API_KEY"] = "test-api-key"
os.environ["AUTH_SECRET_KEY"] = "test-secret"
os.environ["DATABASE_URL"] = "sqlite:///./test_auth.db"


@pytest.fixture(scope="module")
def client():
    from unittest.mock import patch
    with patch("run_pipeline.FeatureAwarePipeline.initialize_components"):
        from src.api.main import app
        from fastapi.testclient import TestClient
        with TestClient(app) as c:
            yield c


def test_signup_and_login(client):
    r = client.post("/auth/signup", json={
        "email": "user@example.com",
        "password": "supersecret123",
        "full_name": "Test User",
    })
    assert r.status_code in (201, 409), r.text

    r = client.post("/auth/login", json={
        "email": "user@example.com",
        "password": "supersecret123",
    })
    assert r.status_code == 200
    body = r.json()
    assert "access_token" in body
    assert body["user"]["email"] == "user@example.com"


def test_me_requires_auth(client):
    r = client.get("/auth/me")
    assert r.status_code == 401


def test_me_with_token(client):
    login = client.post("/auth/login", json={
        "email": "user@example.com",
        "password": "supersecret123",
    })
    token = login.json()["access_token"]
    r = client.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    assert r.json()["email"] == "user@example.com"


def test_runs_list_requires_auth(client):
    r = client.get("/runs")
    assert r.status_code == 401