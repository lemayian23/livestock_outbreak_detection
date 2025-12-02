"""
Database operations for livestock health data
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from .models import HealthMetric, Livestock, OutbreakAlert

def save_metrics(session: Session, metrics_df: pd.DataFrame):
    """
    Save health metrics to database
    
    Args:
        session: SQLAlchemy session
        metrics_df: DataFrame with health metrics
    """
    for _, row in metrics_df.iterrows():
        # Check if record already exists
        existing = session.query(HealthMetric).filter_by(
            tag_id=row['tag_id'],
            date=row['date']
        ).first()
        
        if existing:
            # Update existing record
            for key, value in row.items():
                if hasattr(existing, key):
                    setattr(existing, key, value)
        else:
            # Create new record
            metric = HealthMetric(**row.to_dict())
            session.add(metric)
    
    session.commit()

def save_animal(session: Session, animal_data: Dict):
    """
    Save livestock animal record
    
    Args:
        session: SQLAlchemy session
        animal_data: Dictionary with animal data
    """
    existing = session.query(Livestock).filter_by(
        tag_id=animal_data['tag_id']
    ).first()
    
    if existing:
        # Update existing
        for key, value in animal_data.items():
            if hasattr(existing, key):
                setattr(existing, key, value)
    else:
        # Create new
        animal = Livestock(**animal_data)
        session.add(animal)
    
    session.commit()

def save_alerts(session: Session, alert_data: Dict):
    """
    Save outbreak alert
    
    Args:
        session: SQLAlchemy session
        alert_data: Dictionary with alert data
    """
    alert = OutbreakAlert(**alert_data)
    session.add(alert)
    session.commit()

def get_recent_metrics(session: Session, 
                      days: int = 30,
                      farm_id: Optional[str] = None,
                      animal_type: Optional[str] = None) -> pd.DataFrame:
    """
    Get recent health metrics
    
    Args:
        session: SQLAlchemy session
        days: Number of days to look back
        farm_id: Optional filter by farm
        animal_type: Optional filter by animal type
        
    Returns:
        DataFrame with health metrics
    """
    query = session.query(HealthMetric)
    
    # Filter by date
    cutoff_date = datetime.now() - timedelta(days=days)
    query = query.filter(HealthMetric.date >= cutoff_date)
    
    # Apply filters if provided
    if farm_id:
        query = query.filter(HealthMetric.farm_id == farm_id)
    
    if animal_type:
        query = query.filter(HealthMetric.animal_type == animal_type)
    
    # Execute query
    metrics = query.order_by(HealthMetric.date.desc()).all()
    
    # Convert to DataFrame
    data = []
    for metric in metrics:
        row = {col.name: getattr(metric, col.name) for col in metric.__table__.columns}
        data.append(row)
    
    return pd.DataFrame(data)

def get_active_alerts(session: Session, 
                     severity: Optional[str] = None) -> List[Dict]:
    """
    Get active (unresolved) alerts
    
    Args:
        session: SQLAlchemy session
        severity: Optional filter by severity
        
    Returns:
        List of alert dictionaries
    """
    query = session.query(OutbreakAlert).filter_by(is_resolved=False)
    
    if severity:
        query = query.filter_by(severity=severity)
    
    alerts = query.order_by(OutbreakAlert.created_at.desc()).all()
    
    # Convert to dictionaries
    alert_list = []
    for alert in alerts:
        alert_dict = {col.name: getattr(alert, col.name) for col in alert.__table__.columns}
        alert_list.append(alert_dict)
    
    return alert_list

def mark_alert_resolved(session: Session, alert_id: int):
    """
    Mark an alert as resolved
    
    Args:
        session: SQLAlchemy session
        alert_id: ID of alert to resolve
    """
    alert = session.query(OutbreakAlert).get(alert_id)
    
    if alert:
        alert.is_resolved = True
        alert.end_date = datetime.now()
        session.commit()

def get_animal_summary(session: Session, farm_id: Optional[str] = None) -> Dict:
    """
    Get summary statistics for animals
    
    Args:
        session: SQLAlchemy session
        farm_id: Optional filter by farm
        
    Returns:
        Dictionary with summary statistics
    """
    query = session.query(Livestock).filter_by(is_active=True)
    
    if farm_id:
        query = query.filter_by(farm_id=farm_id)
    
    animals = query.all()
    
    # Count by type
    type_counts = {}
    for animal in animals:
        animal_type = animal.animal_type
        type_counts[animal_type] = type_counts.get(animal_type, 0) + 1
    
    return {
        'total_animals': len(animals),
        'animals_by_type': type_counts,
        'farms': len(set(a.farm_id for a in animals)) if animals else 0
    }