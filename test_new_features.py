#!/usr/bin/env python3
"""
Test script for new features: Isolation Forest and Email Alerts
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np
from datetime import datetime
import tempfile

from src.anomaly_detection.isolation_forest import IsolationForestDetector
from src.anomaly_detection.ensemble import EnsembleDetector
from src.notification.email_sender import EmailAlertSender

def test_isolation_forest():
    """Test Isolation Forest anomaly detection"""
    print("Testing Isolation Forest Detector...")
    print("-" * 50)
    
    # Create test data
    np.random.seed(42)
    n_samples = 100
    
    # Normal data
    normal_temp = np.random.normal(38.5, 0.5, n_samples)
    normal_hr = np.random.normal(70, 5, n_samples)
    normal_activity = np.random.normal(1.0, 0.2, n_samples)
    
    # Add some anomalies
    anomalies_temp = np.random.uniform(40.5, 42.0, 5)
   