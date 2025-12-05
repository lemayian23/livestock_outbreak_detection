#!/usr/bin/env python3
"""
Main pipeline for offline disease outbreak detection
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

from src.utils.config import Config
from src.database.models import DatabaseManager
from src.data_collection.simulator import DataSimulator
from src.anomaly_detection.detector import AnomalyDetector
from src.database.operations import save_metrics, save_alerts
from src.notification.manager import NotificationManager

class OutbreakDetectionPipeline:
    def __init__(self, config_path: str = None):
        self.config = Config(config_path)
        self.db_manager = DatabaseManager(self.config.db_config.path)
        self.detector = AnomalyDetector(self.config)
        self.notification_manager = NotificationManager(self.config.raw_config)
        
    def run_simulation(self):
        """Run complete simulation pipeline"""
        print("=" * 60)
        print("Livestock Outbreak Detection MVP")
        print(f"Detection Method: {self.config.anomaly_config.method}")
        print("=" * 60)
        
        # Step 1: Generate simulated data
        print("\n1. Generating simulated data...")
        simulator = DataSimulator(self.config)
        data = simulator.generate_test_data(n_animals=50, n_days=90)
        print(f"   Generated {len(data)} records for 50 animals over 90 days")
        
        # Step 2: Detect anomalies
        print("\n2. Running anomaly detection...")
        result_df = self.detector.detect(data)
        
        n_anomalies = result_df['is_anomaly'].sum()
        anomaly_rate = n_anomalies/len(result_df)*100 if len(result_df) > 0 else 0
        print(f"   Detected {n_anomalies} anomalous records ({anomaly_rate:.1f}%)")
        
        # Step 3: Detect clusters
        print("\n3. Detecting outbreak clusters...")
        clusters = self.detector.detect_outbreaks(result_df, time_window='7D')
        
        if clusters:
            print(f"   Found {len(clusters)} potential outbreak clusters:")
            for i, cluster in enumerate(clusters, 1):
                severity = self._determine_severity(cluster)
                print(f"   Cluster {i}: {cluster['farm_id']} - "
                      f"{cluster['affected_animals']} animals - "
                      f"Severity: {severity}")
        else:
            print("   No outbreak clusters detected")
        
        # Step 4: Save to database
        print("\n4. Saving results to database...")
        with self.db_manager.get_session() as session:
            save_metrics(session, result_df)
            
            if clusters:
                for cluster in clusters:
                    severity = self._determine_severity(cluster)
                    
                    alert_data = {
                        'farm_id': cluster['farm_id'],
                        'alert_type': 'cluster',
                        'severity': severity,
                        'description': f"Outbreak cluster detected: "
                                     f"{cluster['affected_animals']} animals showing anomalies",
                        'affected_animals': cluster['affected_animals'],
                        'start_date': cluster['start_date'],
                        'end_date': cluster['end_date'],
                        'animal_types': cluster.get('animal_types', []),
                        'detection_methods': cluster.get('detection_methods', 
                                                        [self.config.anomaly_config.method]),
                        'avg_anomaly_score': cluster.get('avg_anomaly_score', 0),
                        'ensemble_score': cluster.get('ensemble_score', 0),
                        'features_contributing': cluster.get('features_contributing', [])
                    }
                    
                    # Save to database
                    save_alerts(session, alert_data)
                    
                    # Send email notification for high severity alerts
                    if severity in ['high', 'critical']:
                        print(f"   Sending alert for {severity} severity cluster...")
                        notification_result = self.notification_manager.send_outbreak_alert(
                            alert_data
                        )
                        
                        if notification_result.get('channels', {}).get('email', {}).get('success'):
                            print(f"     ✓ Email alert sent")
                        else:
                            print(f"     ✗ Email alert failed")
        
        # Step 5: Generate reports
        print("\n5. Generating reports...")
        report_file = self.generate_report(result_df, clusters)
        
        # Step 6: Send daily report if configured
        if hasattr(self.notification_manager, 'email_sender') and self.notification_manager.email_sender:
            print("\n6. Sending daily report...")
            report_data = self._prepare_daily_report_data(result_df, clusters)
            report_result = self.notification_manager.send_daily_report(
                report_data, 
                report_file
            )
            
            if report_result.get('channels', {}).get('email', {}).get('success'):
                print("   ✓ Daily report email sent")
            else:
                print("   ✗ Daily report email failed")
        
        print("\n" + "=" * 60)
        print("Pipeline completed successfully!")
        print("=" * 60)
        
        return result_df, clusters
    
    def _determine_severity(self, cluster: dict) -> str:
        """Determine alert severity based on cluster characteristics"""
        n_animals = cluster['affected_animals']
        score = cluster.get('avg_anomaly_score', 0)
        ensemble_score = cluster.get('ensemble_score', 0)
        
        # Use ensemble score if available
        if ensemble_score > 0:
            if ensemble_score >= 7.0 or n_animals >= 10:
                return 'critical'
            elif ensemble_score >= 5.0 or n_animals >= 5:
                return 'high'
            elif ensemble_score >= 3.0 or n_animals >= 3:
                return 'medium'
            else:
                return 'low'
        else:
            # Fallback to original logic
            if n_animals >= 10 or score >= 5.0:
                return 'critical'
            elif n_animals >= 5 or score >= 4.0:
                return 'high'
            elif n_animals >= 3 or score >= 3.0:
                return 'medium'
            else:
                return 'low'
    
    def _prepare_daily_report_data(self, df: pd.DataFrame, clusters: list) -> dict:
        """Prepare data for daily report"""
        total_animals = df['tag_id'].nunique()
        total_records = len(df)
        anomaly_count = df['is_anomaly'].sum()
        farms_monitored = df['farm_id'].nunique() if 'farm_id' in df.columns else 1
        
        # Animal type distribution
        animal_types = {}
        if 'animal_type' in df.columns:
            animal_counts = df['animal_type'].value_counts()
            animal_types = animal_counts.to_dict()
        
        # Recent alerts (last 7 days)
        recent_alerts = []
        if clusters:
            # Filter clusters from last 7 days
            week_ago = datetime.now() - timedelta(days=7)
            for cluster in clusters:
                if 'start_date' in cluster and cluster['start_date'] >= week_ago:
                    recent_alerts.append({
                        'farm_id': cluster.get('farm_id', 'Unknown'),
                        'severity': self._determine_severity(cluster),
                        'affected_animals': cluster.get('affected_animals', 0),
                        'created_at': cluster.get('start_date', datetime.now())
                    })
        
        return {
            'total_animals': total_animals,
            'total_records': total_records,
            'anomaly_count': anomaly_count,
            'anomaly_rate': (anomaly_count / total_records * 100) if total_records > 0 else 0,
            'farms_monitored': farms_monitored,
            'animal_types': animal_types,
            'recent_alerts': recent_alerts
        }
    
    def generate_report(self, df: pd.DataFrame, clusters: list):
        """Generate HTML report and return file path"""
        from src.visualization.dashboard import HealthDashboard
        
        dashboard = HealthDashboard()
        
        # Create HTML report
        html_content = dashboard.create_summary_report(df, clusters)
        
        # Save report
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = dashboard.save_report(
            html_content, 
            filename=f"health_report_{timestamp}.html"
        )
        
        print(f"   Report saved to: {report_file}")
        return report_file

if __name__ == "__main__":
    # Create necessary directories
    os.makedirs('data', exist_ok=True)
    os.makedirs('outputs/reports', exist_ok=True)
    os.makedirs('outputs/alerts', exist_ok=True)
    os.makedirs('models/saved_models', exist_ok=True)
    
    # Run pipeline
    pipeline = OutbreakDetectionPipeline()
    pipeline.run_simulation()