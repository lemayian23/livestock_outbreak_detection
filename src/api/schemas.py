"""
Pydantic request/response models for the API.
"""
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


# ---------- Request models ----------

class LivestockRecord(BaseModel):
    """A single livestock health record matching daily_health_metrics schema."""
    farm_id: str = Field(..., description="Farm identifier (e.g. FARM-0001)")
    date: str = Field(..., description="ISO date or YYYY-MM-DD")
    animal_type: str = Field(..., description="cattle|swine|poultry|sheep|goat")
    total_animals: int = Field(..., ge=0)
    sick_animals: int = Field(0, ge=0)
    deceased_animals: int = Field(0, ge=0)
    avg_temperature: float = Field(..., ge=0, le=60)
    feed_intake_percent: float = Field(100.0, ge=0, le=200)
    water_intake_percent: float = Field(100.0, ge=0, le=200)
    activity_level: float = Field(5.0, ge=0, le=10)
    location_lat: Optional[float] = None
    location_lon: Optional[float] = None
    tag_id: Optional[str] = None

    @field_validator("animal_type")
    @classmethod
    def check_animal_type(cls, v: str) -> str:
        allowed = {"cattle", "swine", "poultry", "sheep", "goat"}
        if v.lower() not in allowed:
            raise ValueError(f"animal_type must be one of {sorted(allowed)}")
        return v.lower()


class DetectRequest(BaseModel):
    """Payload for POST /v1/detect."""
    records: List[LivestockRecord] = Field(..., min_length=1)
    return_html_report: bool = False


class ValidateRequest(BaseModel):
    """Payload for POST /v1/validate."""
    records: List[LivestockRecord] = Field(..., min_length=1)
    schema_name: str = "daily_health_metrics"


# ---------- Response models ----------

class AnomalyOut(BaseModel):
    farm_id: Optional[str] = None
    animal_type: Optional[str] = None
    date: Optional[str] = None
    severity: Optional[str] = None
    score: Optional[float] = None
    description: Optional[str] = None


class DetectResponse(BaseModel):
    run_id: str
    success: bool
    records_processed: int
    anomalies_detected: int
    anomalies: List[AnomalyOut] = []
    warnings: List[str] = []
    errors: List[str] = []
    quality_score: Optional[float] = None
    report_path: Optional[str] = None
    duration_ms: float
    features_used: List[str] = []


class ValidateResponse(BaseModel):
    schema_name: str
    is_valid: bool
    total_rows: int
    schema_errors: int
    schema_warnings: int
    custom_rule_errors: int
    custom_rule_warnings: int
    quality_score: Optional[float] = None
    quality_grade: Optional[str] = None
    details: Dict[str, Any] = {}


class HealthResponse(BaseModel):
    status: str
    env: str
    version: str
    timestamp: str


class FeatureInfo(BaseModel):
    name: str
    enabled: bool
    category: str
    state: str
    description: str


class ErrorResponse(BaseModel):
    error: str
    code: str
    detail: Optional[str] = None