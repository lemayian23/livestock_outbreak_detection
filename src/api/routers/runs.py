"""
Runs endpoints: list, detail, download report.
"""
import json
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from ..auth.dependencies import get_current_user
from ..database import get_db
from ..models import Anomaly, Report, Run, User
from pydantic import BaseModel


router = APIRouter(prefix="/runs", tags=["runs"])


class AnomalyOut(BaseModel):
    id: str
    farm_id: Optional[str] = None
    animal_type: Optional[str] = None
    date: Optional[str] = None
    severity: Optional[str] = None
    score: Optional[float] = None
    description: Optional[str] = None


class RunSummary(BaseModel):
    id: str
    status: str
    records_submitted: int
    records_processed: int
    anomalies_detected: int
    quality_score: Optional[float] = None
    duration_ms: Optional[float] = None
    created_at: str
    completed_at: Optional[str] = None


class RunDetail(RunSummary):
    error_message: Optional[str] = None
    anomalies: List[AnomalyOut] = []
    reports: List[dict] = []


class RunListResponse(BaseModel):
    items: List[RunSummary]
    total: int
    limit: int
    offset: int


def _summary(r: Run) -> RunSummary:
    return RunSummary(
        id=r.id,
        status=r.status,
        records_submitted=r.records_submitted,
        records_processed=r.records_processed,
        anomalies_detected=r.anomalies_detected,
        quality_score=r.quality_score,
        duration_ms=r.duration_ms,
        created_at=r.created_at.isoformat() if r.created_at else "",
        completed_at=r.completed_at.isoformat() if r.completed_at else None,
    )


@router.get("", response_model=RunListResponse)
def list_runs(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    status_filter: Optional[str] = Query(None, alias="status"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """List the current user's runs, newest first."""
    q = db.query(Run).filter(Run.user_id == current_user.id)
    if status_filter:
        q = q.filter(Run.status == status_filter)

    total = q.count()
    items = q.order_by(Run.created_at.desc()).limit(limit).offset(offset).all()
    return RunListResponse(
        items=[_summary(r) for r in items],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{run_id}", response_model=RunDetail)
def get_run(
    run_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return a single run with anomalies and reports."""
    run = db.query(Run).filter(Run.id == run_id, Run.user_id == current_user.id).first()
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Run not found", "code": "RUN_NOT_FOUND"},
        )

    anomalies = db.query(Anomaly).filter(Anomaly.run_id == run.id).all()
    reports = db.query(Report).filter(Report.run_id == run.id).all()

    detail = RunDetail(
        **_summary(run).model_dump(),
        error_message=run.error_message,
        anomalies=[
            AnomalyOut(
                id=a.id,
                farm_id=a.farm_id,
                animal_type=a.animal_type,
                date=a.date,
                severity=a.severity,
                score=a.score,
                description=a.description,
            )
            for a in anomalies
        ],
        reports=[
            {
                "id": r.id,
                "format": r.format,
                "size_bytes": r.size_bytes,
                "created_at": r.created_at.isoformat() if r.created_at else "",
            }
            for r in reports
        ],
    )
    return detail


@router.delete("/{run_id}", status_code=204)
def delete_run(
    run_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    run = db.query(Run).filter(Run.id == run_id, Run.user_id == current_user.id).first()
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Run not found", "code": "RUN_NOT_FOUND"},
        )
    db.delete(run)
    db.commit()
    return None