#!/usr/bin/env python3
"""
Test script for export functionality
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.export.exporter import DataExporter

def test_export_functionality():
    """Test the export functionality"""
    print("Testing Data Export Feature...")
    print("=" * 50)
    
    # Create exporter
    exporter = DataExporter()
    
    # Create test data
    print("\n1. Creating test data...")
    
    dates = pd.date_range(start='2024-01-01', periods=20, freq='D')
    
    # Test anomaly data
    anomalies_df = pd.DataFrame({
        'tag_id': [f'ANM{str(i).zfill(3)}' for i in range(20)],
        'date': dates,
        'animal_type': np.random.choice(['cattle', 'sheep', 'goat'], 20),
        'farm_id': np.random.choice(['FARM1', 'FARM2', 'FARM3'], 20),
        'temperature': np.random.normal(38.5, 1.5, 20),
        'heart_rate': np.random.normal(70, 10, 20),
        'is_anomaly': np.random.choice([True, False], 20, p=[0.3, 0.7]),
        'anomaly_score': np.random.uniform(0, 10, 20)
    })
    
    # Test alert data
    alerts_data = [
        {
            'farm_id': 'FARM1',
            'severity': 'high',
            'affected_animals': 5,
            'description': 'Elevated temperatures detected',
            'created_at': datetime.now() - timedelta(days=1)
        },
        {
            'farm_id': 'FARM2',
            'severity': 'medium',
            'affected_animals': 3,
            'description': 'Reduced activity levels',
            'created_at': datetime.now() - timedelta(days=3)
        }
    ]
    
    print(f"  Created {len(anomalies_df)} test records")
    print(f"  Created {len(alerts_data)} test alerts")
    
    # Test 1: Export anomalies
    print("\n2. Testing anomaly export...")
    anomaly_files = exporter.export_anomalies(anomalies_df)
    print(f"  Exported to: {list(anomaly_files.keys())}")
    
    # Test 2: Export alerts
    print("\n3. Testing alert export...")
    alert_files = exporter.export_alerts(alerts_data)
    print(f"  Exported to: {list(alert_files.keys())}")
    
    # Test 3: Export health metrics
    print("\n4. Testing health metrics export...")
    health_files = exporter.export_health_metrics(anomalies_df, 'test_range')
    print(f"  Exported