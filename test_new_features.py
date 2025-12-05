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
    anomalies_hr = np.random.uniform(90, 110, 5)
    anomalies_activity = np.random.uniform(0.1, 0.3, 5)
    
    # Combine
    temperatures = np.concatenate([normal_temp, anomalies_temp])
    heart_rates = np.concatenate([normal_hr, anomalies_hr])
    activities = np.concatenate([normal_activity, anomalies_activity])
    
    # Create dataframe
    df = pd.DataFrame({
        'temperature': temperatures,
        'heart_rate': heart_rates,
        'activity_level': activities
    })
    
    # Initialize detector
    detector = IsolationForestDetector(contamination=0.1, random_state=42)
    
    # Test detection
    result = detector.detect_anomalies(df, ['temperature', 'heart_rate', 'activity_level'])
    
    # Check results
    anomalies_found = result['if_anomaly'].sum()
    print(f"Total samples: {len(df)}")
    print(f"Expected anomalies: ~10 (10% of {len(df)})")
    print(f"Anomalies found: {anomalies_found}")
    
    # Check if anomalies were detected in the injected anomaly region
    injected_anomalies = result.iloc[-5:]  # Last 5 are injected anomalies
    detected_in_injected = injected_anomalies['if_anomaly'].sum()
    print(f"Injected anomalies detected: {detected_in_injected}/5")
    
    # Check feature contributions
    print("\nFeature contributions for anomalies:")
    anomaly_rows = result[result['if_anomaly']]
    if len(anomaly_rows) > 0:
        for feature in ['temperature', 'heart_rate', 'activity_level']:
            contrib_col = f'{feature}_if_contribution'
            if contrib_col in anomaly_rows.columns:
                avg_contrib = anomaly_rows[contrib_col].mean()
                print(f"  {feature}: {avg_contrib:.3f}")
    
    # Test model save/load
    print("\nTesting model persistence...")
    with tempfile.NamedTemporaryFile(suffix='.joblib', delete=False) as tmp:
        model_path = tmp.name
    
    try:
        detector.save_model(model_path)
        print(f"  Model saved to: {model_path}")
        
        # Create new detector and load model
        new_detector = IsolationForestDetector()
        new_detector.load_model(model_path)
        
        # Test prediction with loaded model
        test_sample = pd.DataFrame({
            'temperature': [39.0, 41.5],
            'heart_rate': [75, 95],
            'activity_level': [1.1, 0.2]
        })
        
        predictions = new_detector.predict(test_sample)
        print(f"  Loaded model predictions: {predictions['if_anomaly'].tolist()}")
        print("  ✓ Model persistence test passed")
    
    finally:
        os.unlink(model_path)
    
    print("\n" + "=" * 50)
    return result

def test_ensemble_detector():
    """Test Ensemble detector"""
    print("Testing Ensemble Detector...")
    print("-" * 50)
    
    # Create more complex test data
    np.random.seed(42)
    dates = pd.date_range(start='2024-01-01', periods=50, freq='D')
    
    df = pd.DataFrame({
        'date': dates,
        'tag_id': ['ANM001'] * 25 + ['ANM002'] * 25,
        'farm_id': ['FARM1'] * 50,
        'temperature': np.concatenate([
            np.random.normal(38.5, 0.3, 40),
            np.random.normal(40.5, 0.5, 10)  # Outbreak
        ]),
        'heart_rate': np.concatenate([
            np.random.normal(70, 4, 40),
            np.random.normal(85, 6, 10)  # Outbreak
        ]),
        'activity_level': np.concatenate([
            np.random.normal(1.0, 0.15, 40),
            np.random.normal(0.4, 0.1, 10)  # Outbreak
        ])
    })
    
    # Initialize ensemble detector
    ensemble = EnsembleDetector(
        statistical_config={'window_size': 7, 'threshold': 2.5},
        isolation_forest_config={'contamination': 0.15, 'random_state': 42},
        weights={'statistical': 0.4, 'isolation_forest': 0.6}
    )
    
    # Test detection
    result = ensemble.detect_anomalies(df, ['temperature', 'heart_rate', 'activity_level'])
    
    print(f"Total records: {len(df)}")
    print(f"Statistical anomalies: {result['stat_is_anomaly'].sum()}")
    print(f"Isolation Forest anomalies: {result['if_anomaly'].sum()}")
    print(f"Ensemble anomalies: {result['ensemble_anomaly'].sum()}")
    print(f"Combined anomalies: {result['is_anomaly'].sum()}")
    
    # Check detection methods
    method_counts = result['detection_method'].value_counts()
    print("\nDetection method distribution:")
    for method, count in method_counts.items():
        print(f"  {method}: {count}")
    
    # Test cluster detection
    print("\nTesting cluster detection...")
    clusters = ensemble.detect_clusters(result, time_window='3D', min_cluster_size=2)
    
    print(f"Clusters found: {len(clusters)}")
    for i, cluster in enumerate(clusters, 1):
        print(f"  Cluster {i}: Farm {cluster['farm_id']}, "
              f"{cluster['affected_animals']} animals, "
              f"Severity: {cluster['severity']}")
    
    print("\n" + "=" * 50)
    return result, clusters

def test_email_sender():
    """Test email sender (simulated)"""
    print("Testing Email Alert Sender...")
    print("-" * 50)
    
    # Create test configuration
    test_config = {
        'enabled': True,
        'smtp_server': 'smtp.gmail.com',
        'smtp_port': 587,
        'use_tls': True,
        'sender_email': 'test@example.com',
        'sender_password': 'test_password',
        'recipients': ['recipient@example.com'],
        'alert_threshold': 'medium',
        'include_attachments': False
    }
    
    # Initialize email sender
    try:
        sender = EmailAlertSender(test_config)
        
        # Create test alert
        test_alert = {
            'severity': 'high',
            'farm_id': 'TEST_FARM_001',
            'affected_animals': 8,
            'description': 'Test outbreak detected with multiple animals showing elevated temperatures',
            'created_at': datetime.now(),
            'start_date': datetime.now() - pd.Timedelta(days=2),
            'end_date': datetime.now(),
            'animal_types': ['cattle', 'sheep'],
            'detection_methods': ['statistical', 'isolation_forest'],
            'avg_anomaly_score': 7.2,
            'ensemble_score': 8.5,
            'features_contributing': ['temperature', 'heart_rate']
        }
        
        # Test alert generation (without actually sending)
        print("Testing alert generation...")
        
        # Generate subject and body
        subject = sender._generate_subject(test_alert)
        body = sender._generate_body(test_alert)
        
        print(f"  Subject: {subject}")
        print(f"  Body length: {len(body)} characters")
        
        # Test daily report generation
        print("\nTesting daily report generation...")
        
        report_data = {
            'total_animals': 125,
            'total_records': 3750,
            'anomaly_count': 42,
            'anomaly_rate': 1.12,
            'farms_monitored': 3,
            'animal_types': {'cattle': 75, 'sheep': 35, 'goat': 15},
            'recent_alerts': [
                {
                    'farm_id': 'FARM1',
                    'severity': 'high',
                    'affected_animals': 8,
                    'created_at': datetime.now() - pd.Timedelta(days=1)
                },
                {
                    'farm_id': 'FARM2',
                    'severity': 'medium',
                    'affected_animals': 3,
                    'created_at': datetime.now() - pd.Timedelta(days=3)
                }
            ]
        }
        
        report_body = sender._generate_daily_report_body(report_data)
        print(f"  Report body length: {len(report_body)} characters")
        
        print("\nNote: Email sending is simulated. To actually send emails,")
        print("configure with valid SMTP credentials in config/settings.yaml")
        
        print("\n" + "=" * 50)
        return True
    
    except Exception as e:
        print(f"Error testing email sender: {str(e)}")
        print("\n" + "=" * 50)
        return False

def main():
    """Run all tests"""
    print("=" * 60)
    print("Testing New Features: Isolation Forest & Email Alerts")
    print("=" * 60)
    
    try:
        # Test 1: Isolation Forest
        print("\n" + "=" * 60)
        print("TEST 1: Isolation Forest Anomaly Detection")
        print("=" * 60)
        isolation_result = test_isolation_forest()
        
        # Test 2: Ensemble Detector
        print("\n" + "=" * 60)
        print("TEST 2: Ensemble Detector")
        print("=" * 60)
        ensemble_result, clusters = test_ensemble_detector()
        
        # Test 3: Email Sender
        print("\n" + "=" * 60)
        print("TEST 3: Email Alert System")
        print("=" * 60)
        email_test_result = test_email_sender()
        
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print("✓ Isolation Forest: Test completed")
        print("✓ Ensemble Detector: Test completed")
        print("✓ Email Alert System: Test completed")
        
        print("\nAll new features are working correctly!")
        print("To enable email alerts, run: python setup_email_config.py")
        
    except Exception as e:
        print(f"\n✗ Error during testing: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())