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
from src.anomaly_detection.statistical import StatisticalDetector
from src.database.operations import save_metrics, save_alerts

class OutbreakDetectionPipeline:
    def __init__(self, config_path: str = None):
        self.config = Config(config_path)
        self.db_manager = DatabaseManager(self.config.db_config.path)
        self.detector = StatisticalDetector(
            window_size=self.config.anomaly_config.window_size,
            threshold=self.config.anomaly_config.threshold
        )
        
    def run_simulation(self):
        """Run complete simulation pipeline"""
        print("=" * 60)
        print("Livestock Outbreak Detection MVP")
        print("=" * 60)
        
        # Step 1: Generate simulated data
        print("\n1. Generating simulated data...")
        simulator = DataSimulator(self.config)
        data = simulator.generate_test_data(n_animals=50, n_days=90)
        print(f"   Generated {len(data)} records for 50 animals over 90 days")
        
        # Step 2: Detect anomalies
        print("\n2. Running anomaly detection...")
        metrics = ['temperature', 'heart_rate', 'activity_level']
        result_df = self.detector.detect_anomalies(data, metrics)
        
        n_anomalies = result_df['is_anomaly'].sum()
        print(f"   Detected {n_anomalies} anomalous records ({n_anomalies/len(result_df)*100:.1f}%)")
        
        # Step 3: Detect clusters
        print("\n3. Detecting outbreak clusters...")
        clusters = self.detector.detect_clusters(result_df, time_window='7D', min_cluster_size=3)
        
        if clusters:
            print(f"   Found {len(clusters)} potential outbreak clusters:")
            for i, cluster in enumerate(clusters, 1):
                print(f"   Cluster {i}: {cluster['farm_id']} - "
                      f"{cluster['affected_animals']} animals affected")
        else:
            print("   No outbreak clusters detected")
        
        # Step 4: Save to database
        print("\n4. Saving results to database...")
        with self.db_manager.get_session() as session:
            save_metrics(session, result_df)
            
            if clusters:
                for cluster in clusters:
                    alert = {
                        'farm_id': cluster['farm_id'],
                        'alert_type': 'cluster',
                        'severity': self._determine_severity(cluster),
                        'description': f"Cluster outbreak detected: "
                                     f"{cluster['affected_animals']} animals showing anomalies",
                        'affected_animals': cluster['affected_animals'],
                        'start_date': cluster['start_date'],
                        'metrics_affected': ', '.join(cluster['metrics_affected'])
                    }
                    save_alerts(session, alert)
        
        print("\n5. Generating reports...")
        self.generate_report(result_df, clusters)
        
        print("\n" + "=" * 60)
        print("Pipeline completed successfully!")
        print("=" * 60)
        
        return result_df, clusters
    
    def _determine_severity(self, cluster: dict) -> str:
        """Determine alert severity based on cluster characteristics"""
        n_animals = cluster['affected_animals']
        score = cluster['avg_anomaly_score']
        
        if n_animals >= 10 or score >= 5.0:
            return 'critical'
        elif n_animals >= 5 or score >= 4.0:
            return 'high'
        elif n_animals >= 3 or score >= 3.0:
            return 'medium'
        else:
            return 'low'
    
    def generate_report(self, df: pd.DataFrame, clusters: list):
        """Generate simple text report"""
        os.makedirs('outputs/reports', exist_ok=True)
        
        report_path = f"outputs/reports/report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(report_path, 'w') as f:
            f.write("Livestock Health Outbreak Detection Report\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Generated: {datetime.now()}\n")
            f.write(f"Total records: {len(df)}\n")
            f.write(f"Anomalous records: {df['is_anomaly'].sum()}\n")
            f.write(f"Animals monitored: {df['tag_id'].nunique()}\n")
            f.write(f"Farms monitored: {df['farm_id'].nunique()}\n\n")
            
            if clusters:
                f.write("DETECTED OUTBREAK CLUSTERS:\n")
                f.write("-" * 30 + "\n")
                for i, cluster in enumerate(clusters, 1):
                    f.write(f"\nCluster {i}:\n")
                    f.write(f"  Farm: {cluster['farm_id']}\n")
                    f.write(f"  Period: {cluster['start_date'].date()} to "
                           f"{cluster['end_date'].date()}\n")
                    f.write(f"  Affected animals: {cluster['affected_animals']}\n")
                    f.write(f"  Average anomaly score: {cluster['avg_anomaly_score']:.2f}\n")
                    f.write(f"  Animal types: {', '.join(cluster['animal_types'])}\n")
                    f.write(f"  Metrics affected: {', '.join(cluster['metrics_affected'])}\n")
            else:
                f.write("No outbreak clusters detected.\n")
        
        print(f"   Report saved to: {report_path}")

if __name__ == "__main__":
    # Create necessary directories
    os.makedirs('data', exist_ok=True)
    os.makedirs('outputs/reports', exist_ok=True)
    os.makedirs('outputs/alerts', exist_ok=True)
    
    # Run pipeline
    pipeline = OutbreakDetectionPipeline()
    pipeline.run_simulation()