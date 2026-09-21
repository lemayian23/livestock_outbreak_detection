"""
Tests for the FastAPI wrapper using TestClient.
"""
import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(autouse=True)
def _api_env(monkeypatch):
    """Set API env vars for every test in this module only."""
    monkeypatch.setenv("API_KEY", "test-api-key")
    monkeypatch.setenv("APP_ENV", "testing")
    yield


@pytest.fixture(scope="function")
def client(_api_env):
    """Fresh TestClient per test, with pipeline init mocked out."""
    with patch("run_pipeline.FeatureAwarePipeline.initialize_components"):
        from src.api.main import app
        from fastapi.testclient import TestClient
        with TestClient(app) as c:
            yield c


HEADERS = {"X-API-Key": "test-api-key"}

SAMPLE_RECORDS = [
    {
        "farm_id": "FARM-0001",
        "date": "2026-09-20",
        "animal_type": "cattle",
        "total_animals": 120,
        "sick_animals": 4,
        "deceased_animals": 1,
        "avg_temperature": 38.6,
        "feed_intake_percent": 88.0,
        "water_intake_percent": 92.0,
        "activity_level": 7.2,
    }
]


def test_root(client):
    r = client.get("/")
    assert r.status_code == 200
    assert r.json()["service"] == "livestock-outbreak-detection"


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "version" in body


def test_detect_requires_auth(client):
    r = client.post("/v1/detect", json={"records": SAMPLE_RECORDS})
    assert r.status_code == 401


def test_detect_invalid_payload(client):
    r = client.post(
        "/v1/detect",
        headers=HEADERS,
        json={"records": [{"farm_id": "FARM-1"}]},
    )
    assert r.status_code == 422


def test_detect_with_invalid_animal_type(client):
    bad = dict(SAMPLE_RECORDS[0])
    bad["animal_type"] = "dragon"
    r = client.post("/v1/detect", headers=HEADERS, json={"records": [bad]})
    assert r.status_code == 422


def test_validate_endpoint(client):
    r = client.post(
        "/v1/validate",
        headers=HEADERS,
        json={"records": SAMPLE_RECORDS},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["schema_name"] == "daily_health_metrics"
    assert "is_valid" in body
    assert body["total_rows"] == 1


def test_features_endpoint(client):
    r = client.get("/v1/features", headers=HEADERS)
    assert r.status_code == 200
    features = r.json()
    assert isinstance(features, list)
    assert len(features) > 0
    assert "name" in features[0]


def test_detect_success_shape(client):
    """Detection runs end-to-end with pipeline.run() mocked."""
    with patch("run_pipeline.FeatureAwarePipeline.run") as mock_run:
        mock_run.return_value = {
            "success": True,
            "anomalies": [],
            "warnings": [],
            "errors": [],
            "features_used": ["data_validation"],
            "report_path": None,
            "quality_report": {"quality_score": 0.95},
        }
        r = client.post(
            "/v1/detect",
            headers=HEADERS,
            json={"records": SAMPLE_RECORDS},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["success"] is True
        assert body["records_processed"] == 1
        assert "run_id" in body
        assert "duration_ms" in body