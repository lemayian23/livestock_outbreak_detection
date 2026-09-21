"""
API route handlers.
"""
import time
import uuid
from datetime import datetime, timezone
from typing import List

import pandas as pd
from fastapi import APIRouter, Depends, File, Request, UploadFile, status

from custom_logging.structured_logger import get_structured_logger
from data_validation.validator import get_data_validator

from .auth import require_api_key
from .dependencies import get_pipeline
from .schemas import (
    AnomalyOut,
    DetectRequest,
    DetectResponse,
    FeatureInfo,
    HealthResponse,
    ValidateRequest,
    ValidateResponse,
)

logger = get_structured_logger()
router = APIRouter()


# ---------- Public endpoints ----------

@router.get("/", tags=["meta"])
def root():
    from . import __version__
    return {
        "service": "livestock-outbreak-detection",
        "version": __version__,
        "docs": "/docs",
        "health": "/health",
    }


@router.get("/health", response_model=HealthResponse, tags=["meta"])
def health(request: Request):
    env = getattr(request.app.state, "env", "unknown")
    from . import __version__
    return HealthResponse(
        status="ok",
        env=env,
        version=__version__,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


# ---------- Protected endpoints ----------

@router.post(
    "/v1/detect",
    response_model=DetectResponse,
    status_code=status.HTTP_200_OK,
    tags=["detection"],
    dependencies=[Depends(require_api_key)],
)
@router.post(
    "/v1/detect",
    response_model=DetectResponse,
    status_code=status.HTTP_200_OK,
    tags=["detection"],
)
def detect(
    payload: DetectRequest,
    pipeline=Depends(get_pipeline),
    db=Depends(get_db_dep),
    current_user=Depends(get_optional_user),
):
    """Run the full anomaly detection pipeline on submitted records."""
    import uuid
    import json as _json
    from datetime import datetime as _dt, timezone as _tz
    from .models import Run, Anomaly

    run_id = f"run_{_dt.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    start = time.perf_counter()

    logger.set_context(run_id=run_id)
    logger.info(f"Received detect request: {len(payload.records)} records")

    df = pd.DataFrame([r.model_dump() for r in payload.records])

    db_run = None
    if current_user is not None:
        db_run = Run(
            user_id=current_user.id,
            status="running",
            records_submitted=len(payload.records),
        )
        db.add(db_run)
        db.commit()
        db.refresh(db_run)

    try:
        results = pipeline.run(input_data=df)
    except Exception as e:
        logger.error(f"Pipeline failed for run {run_id}", exception=e)
        if db_run:
            db_run.status = "failed"
            db_run.error_message = str(e)[:1000]
            db_run.completed_at = _dt.now(_tz.utc)
            db.commit()
        raise

    duration_ms = (time.perf_counter() - start) * 1000
    anomalies_raw = results.get("anomalies", []) or []

    anomalies: List[AnomalyOut] = []
    for a in anomalies_raw:
        anomalies.append(
            AnomalyOut(
                farm_id=a.get("farm_id"),
                animal_type=a.get("animal_type"),
                date=str(a.get("date") or a.get("timestamp") or ""),
                severity=a.get("severity"),
                score=a.get("anomaly_score") or a.get("score"),
                description=a.get("description"),
            )
        )

    quality_score = None
    qr = results.get("quality_report")
    if isinstance(qr, dict):
        quality_score = qr.get("quality_score")

    if db_run is not None:
        db_run.status = "success" if results.get("success") else "failed"
        db_run.records_processed = len(df)
        db_run.anomalies_detected = len(anomalies)
        db_run.quality_score = quality_score
        db_run.duration_ms = round(duration_ms, 2)
        db_run.completed_at = _dt.now(_tz.utc)
        db.commit()

        for a in anomalies:
            db.add(Anomaly(
                run_id=db_run.id,
                farm_id=a.farm_id,
                animal_type=a.animal_type,
                date=a.date,
                severity=a.severity,
                score=a.score,
                description=a.description,
                raw_json=_json.dumps(a.model_dump(), default=str),
            ))
        db.commit()

    response = DetectResponse(
        run_id=run_id,
        success=bool(results.get("success", False)),
        records_processed=len(df),
        anomalies_detected=len(anomalies),
        anomalies=anomalies,
        warnings=results.get("warnings", []) or [],
        errors=results.get("errors", []) or [],
        quality_score=quality_score,
        report_path=results.get("report_path"),
        duration_ms=round(duration_ms, 2),
        features_used=results.get("features_used", []) or [],
    )

    logger.info(f"Completed run {run_id}: {response.anomalies_detected} anomalies")
    logger.clear_context()
    return response


@router.post(
    "/v1/detect/csv",
    response_model=DetectResponse,
    tags=["detection"],
    dependencies=[Depends(require_api_key)],
)
async def detect_csv(
    file: UploadFile = File(...),
    pipeline=Depends(get_pipeline),
):
    """Upload a CSV file and run detection on it."""
    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    start = time.perf_counter()

    logger.set_context(run_id=run_id)
    logger.info(f"Received CSV upload: {file.filename}")

    contents = await file.read()
    import io
    df = pd.read_csv(io.BytesIO(contents))

    results = pipeline.run(input_data=df)
    duration_ms = (time.perf_counter() - start) * 1000

    anomalies_raw = results.get("anomalies", []) or []
    anomalies = [
        AnomalyOut(
            farm_id=a.get("farm_id"),
            animal_type=a.get("animal_type"),
            date=str(a.get("date") or a.get("timestamp") or ""),
            severity=a.get("severity"),
            score=a.get("anomaly_score") or a.get("score"),
            description=a.get("description"),
        )
        for a in anomalies_raw
    ]

    qr = results.get("quality_report")
    quality_score = qr.get("quality_score") if isinstance(qr, dict) else None

    logger.clear_context()
    return DetectResponse(
        run_id=run_id,
        success=bool(results.get("success", False)),
        records_processed=len(df),
        anomalies_detected=len(anomalies),
        anomalies=anomalies,
        warnings=results.get("warnings", []) or [],
        errors=results.get("errors", []) or [],
        quality_score=quality_score,
        report_path=results.get("report_path"),
        duration_ms=round(duration_ms, 2),
        features_used=results.get("features_used", []) or [],
    )


@router.post(
    "/v1/validate",
    response_model=ValidateResponse,
    tags=["validation"],
    dependencies=[Depends(require_api_key)],
)
def validate(payload: ValidateRequest):
    """Validate records against a schema without running the full pipeline."""
    df = pd.DataFrame([r.model_dump() for r in payload.records])
    validator = get_data_validator()
    report = validator.validate_with_schema(payload.schema_name, df)
    quality = validator.create_data_quality_report(df, payload.schema_name)

    return ValidateResponse(
        schema_name=payload.schema_name,
        is_valid=bool(report.get("is_valid", False)),
        total_rows=len(df),
        schema_errors=report["summary"]["schema_errors"],
        schema_warnings=report["summary"]["schema_warnings"],
        custom_rule_errors=report["summary"]["custom_rule_errors"],
        custom_rule_warnings=report["summary"]["custom_rule_warnings"],
        quality_score=quality.get("quality_score"),
        quality_grade=quality.get("quality_grade"),
        details={"quality_metrics": quality.get("quality_metrics", {})},
    )


@router.get(
    "/v1/features",
    response_model=List[FeatureInfo],
    tags=["meta"],
    dependencies=[Depends(require_api_key)],
)
def list_features():
    """Return current feature toggle states."""
    from utils.feature_manager import get_feature_manager
    fm = get_feature_manager()
    out: List[FeatureInfo] = []
    for name, feat in fm.get_all_features().items():
        out.append(
            FeatureInfo(
                name=name,
                enabled=fm.is_enabled(name),
                category=feat.category,
                state=feat.state.value,
                description=feat.description,
            )
        )
    return out