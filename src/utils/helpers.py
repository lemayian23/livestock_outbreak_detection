"""
Helper functions for the livestock outbreak detection system
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
from typing import Dict, List, Any, Optional
import hashlib

def generate_animal_id(farm_id: str, sequence: int) -> str:
    """
    Generate a unique animal ID
    
    Args:
        farm_id: Farm identifier
        sequence: Sequence number
        
    Returns:
        Unique animal ID
    """
    return f"{farm_id}_{str(sequence).zfill(4)}"

def calculate_health_score(row: pd.Series, config: Any) -> float:
    """
    Calculate overall health score for an animal
    
    Args:
        row: Row with health metrics
        config: Configuration object with normal ranges
        
    Returns:
        Health score (0-100, higher is better)
    """
    score = 100.0
    
    # Temperature scoring
    if 'temperature' in row and 'animal_type' in row:
        temp_range = config.get_normal_range('temperature', row['animal_type'])
        if temp_range and pd.notna(row['temperature']):
            temp = row['temperature']
            # Penalize deviation from normal range
            if temp < temp_range[0] or temp > temp_range[1]:
                deviation = min(abs(temp - temp_range[0]), abs(temp - temp_range[1]))
                score -= deviation * 5
    
    # Heart rate scoring
    if 'heart_rate' in row and 'animal_type' in row:
        hr_range = config.get_normal_range('heart_rate', row['animal_type'])
        if hr_range and pd.notna(row['heart_rate']):
            hr = row['heart_rate']
            if hr < hr_range[0] or hr > hr_range[1]:
                deviation = min(abs(hr - hr_range[0]), abs(hr - hr_range[1]))
                score -= deviation * 2
    
    # Activity level scoring
    if 'activity_level' in row and pd.notna(row['activity_level']):
        activity = row['activity_level']
        normal_activity = 1.0  # Baseline
        deviation = abs(activity - normal_activity)
        score -= deviation * 10
    
    # Ensure score is within bounds
    return max(0.0, min(100.0, score))

def detect_data_drift(reference_data: pd.DataFrame, 
                     current_data: pd.DataFrame,
                     columns: List[str]) -> Dict[str, float]:
    """
    Detect drift in data distributions
    
    Args:
        reference_data: Reference dataset
        current_data: Current dataset
        columns: Columns to check for drift
        
    Returns:
        Dictionary with drift scores for each column
    """
    drift_scores = {}
    
    for col in columns:
        if col not in reference_data.columns or col not in current_data.columns:
            continue
        
        ref_mean = reference_data[col].mean()
        ref_std = reference_data[col].std()
        
        curr_mean = current_data[col].mean()
        curr_std = current_data[col].std()
        
        # Calculate drift score (simplified)
        mean_drift = abs(curr_mean - ref_mean) / (ref_std + 1e-10)
        std_drift = abs(curr_std - ref_std) / (ref_std + 1e-10)
        
        drift_scores[col] = max(mean_drift, std_drift)
    
    return drift_scores

def save_backup(data: Any, backup_type: str, 
                backup_dir: str = './data/backups'):
    """
    Save data backup
    
    Args:
        data: Data to backup
        backup_type: Type of backup (e.g., 'metrics', 'alerts')
        backup_dir: Backup directory
    """
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{backup_type}_backup_{timestamp}.json"
    filepath = os.path.join(backup_dir, filename)
    
    # Convert to serializable format
    if isinstance(data, pd.DataFrame):
        data_dict = data.to_dict('records')
    elif hasattr(data, '__dict__'):
        data_dict = data.__dict__
    else:
        data_dict = data
    
    with open(filepath, 'w') as f:
        json.dump(data_dict, f, indent=2, default=str)
    
    return filepath

def load_latest_backup(backup_type: str, 
                      backup_dir: str = './data/backups') -> Optional[Dict]:
    """
    Load latest backup of specified type
    
    Args:
        backup_type: Type of backup to load
        backup_dir: Backup directory
        
    Returns:
        Loaded data or None if no backup found
    """
    if not os.path.exists(backup_dir):
        return None
    
    # Find backup files
    backup_files = []
    for f in os.listdir(backup_dir):
        if f.startswith(f"{backup_type}_backup_") and f.endswith('.json'):
            backup_files.append(f)
    
    if not backup_files:
        return None
    
    # Get latest backup
    latest_backup = sorted(backup_files)[-1]
    filepath = os.path.join(backup_dir, latest_backup)
    
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    return data

def calculate_data_hash(data: Any) -> str:
    """
    Calculate hash of data for integrity checking
    
    Args:
        data: Data to hash
        
    Returns:
        MD5 hash string
    """
    if isinstance(data, pd.DataFrame):
        data_str = data.to_json()
    elif isinstance(data, dict):
        data_str = json.dumps(data, sort_keys=True)
    else:
        data_str = str(data)
    
    return hashlib.md5(data_str.encode()).hexdigest()

def format_alert_message(alert: Dict) -> str:
    """
    Format alert for display or notification
    
    Args:
        alert: Alert dictionary
        
    Returns:
        Formatted alert message
    """
    severity = alert.get('severity', 'unknown').upper()
    farm_id = alert.get('farm_id', 'Unknown Farm')
    affected = alert.get('affected_animals', 0)
    description = alert.get('description', '')
    
    message = f"[{severity} ALERT] Farm: {farm_id}\n"
    message += f"Affected Animals: {affected}\n"
    message += f"Description: {description}\n"
    
    if 'start_date' in alert:
        message += f"Start: {alert['start_date']}\n"
    
    return message

def validate_date_range(start_date: str, end_date: str) -> bool:
    """
    Validate date range
    
    Args:
        start_date: Start date string
        end_date: End date string
        
    Returns:
        True if valid, False otherwise
    """
    try:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        if start > end:
            return False
        
        if start < pd.Timestamp('2000-01-01'):
            return False
        
        if end > datetime.now() + timedelta(days=365):
            return False
        
        return True
    except:
        return False