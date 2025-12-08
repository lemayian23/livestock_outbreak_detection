#!/usr/bin/env python3
"""
Test script for alert logging system
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.logging.alert_logger import AlertLogger
from datetime import datetime, timedelta
import json

def test_alert_logging():
    """Test the alert logging system"""
    print("Testing Alert Logging System...")
    print("=" * 50)
    
    # Create logger
    logger = AlertLogger()
    
    # Create test alerts
    print("\n1. Creating test alerts...")
    
    test_alerts = [
        {
            'severity': 'critical',
            'farm_id': 'FARM001',
            'message': 'Multiple animals showing high fever',
            'affected_animals': 8,
            'description': 'Temperature anomalies detected across herd'
        },
        {
            'severity': 'high',
            'farm_id': 'FARM002',
            'message': 'Reduced activity levels detected',
            'affected_animals': 3,
            'description': 'Activity monitoring shows decreased movement'
        },
        {
            'severity': 'medium',
            'farm_id': 'FARM001',
            'message': 'Irregular heart rates',
            'affected_animals': 2,
            'description': 'Heart rate variability outside normal range'
        },
        {
            'severity': 'low',
            'farm_id': 'FARM003',
            'message': 'Minor temperature fluctuations',
            'affected_animals': 1,
            'description': 'Single animal showing slight temperature increase'
        }
    ]
    
    # Log alerts
    for alert in test_alerts:
        logger.log_alert(alert)
        print(f"   Logged: {alert['severity']} alert for {alert['farm_id']}")
    
    # Get today's alerts
    print("\n2. Retrieving today's alerts...")
    todays_alerts = logger.get_todays_alerts()
    print(f"   Found {len(todays_alerts)} log entries today")
    
    for entry in todays_alerts[:2]:  # Show first 2
        print(f"   - {entry.strip()}")
    
    # Get recent alerts (JSON)
    print("\n3. Getting recent alerts (JSON)...")
    recent_alerts = logger.get_recent_alerts(days=1)
    print(f"   Found {len(recent_alerts)} detailed alerts")
    
    # Search alerts
    print("\n4. Testing search functionality...")
    
    # Search by severity
    critical_alerts = logger.search_alerts(severity='critical', days=1)
    print(f"   Critical alerts: {len(critical_alerts)}")
    
    # Search by farm
    farm1_alerts = logger.search_alerts(farm_id='FARM001', days=1)
    print(f"   Farm 001 alerts: {len(farm1_alerts)}")
    
    # Search by keyword
    temp_alerts = logger.search_alerts(keyword='temperature', days=1)
    print(f"   Temperature-related alerts: {len(temp_alerts)}")
    
    # Get statistics
    print("\n5. Getting alert statistics...")
    stats = logger.get_alert_stats(days=1)
    print(f"   Total alerts: {stats.get('total_alerts', 0)}")
    print(f"   By severity: {stats.get('by_severity', {})}")
    print(f"   By farm: {stats.get('by_farm', {})}")
    
    # Export alerts
    print("\n6. Testing export functionality...")
    
    # Export to CSV
    csv_file = logger.export_alerts(days=1, format='csv')
    if csv_file and os.path.exists(csv_file):
        print(f"   ✓ CSV export: {csv_file}")
        print(f"   File size: {os.path.getsize(csv_file)} bytes")
    else:
        print("   ✗ CSV export failed")
    
    # Export to JSON
    json_file = logger.export_alerts(days=1, format='json')
    if json_file and os.path.exists(json_file):
        print(f"   ✓ JSON export: {json_file}")
        print(f"   File size: {os.path.getsize(json_file)} bytes")
    else:
        print("   ✗ JSON export failed")
    
    # Get log summary
    print("\n7. Getting log summary...")
    summary = logger.get_log_summary()
    print(summary)
    
    print("\n" + "=" * 50)
    print("Alert logging test completed successfully! ✓")
    
    # Ask about cleanup
    print("\nCleanup test files?")
    cleanup = input("Delete test log files? (y/n): ").lower().strip()
    
    if cleanup == 'y':
        deleted = logger.cleanup_old_logs(days_to_keep=0)  # Delete all
        print(f"Deleted {deleted} log files")
    else:
        print("Test files preserved in outputs/alert_logs/")
    
    return True

if __name__ == "__main__":
    try:
        test_alert_logging()
    except Exception as e:
        print(f"\nError during alert logging test: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)