import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from src.anomaly_detection.statistical import StatisticalDetector

class TestAnomalyDetection(unittest.TestCase):
    
    def setUp(self):
        self.detector = StatisticalDetector(window_size=7, threshold=3.0)
        
        # Create test data
        dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
        self.test_data = pd.DataFrame({
            'date': dates,
            'temperature': np.concatenate([
                np.random.normal(38.5, 0.5, 25),
                np.array([40.5, 41.0, 40.8, 39.5, 38.7])  # Outbreak
            ]),
            'heart_rate': np.random.normal(70, 5, 30)
        })
    
    def test_zscore_calculation(self):
        zscores = self.detector.calculate_zscore(self.test_data['temperature'])
        
        self.assertEqual(len(zscores), len(self.test_data))
        self.assertTrue(all(np.isnan(zscores[:6])))  # First window_size-1 should be NaN
        self.assertFalse(all(np.isnan(zscores[6:])))  # Rest should have values
    
    def test_anomaly_detection(self):
        result = self.detector.detect_anomalies(self.test_data, ['temperature'])
        
        self.assertIn('temperature_zscore', result.columns)
        self.assertIn('temperature_anomaly', result.columns)
        self.assertIn('is_anomaly', result.columns)
        
        # Check that outbreak values are detected
        outbreak_indices = result['temperature'] > 40.0
        self.assertTrue(result.loc[outbreak_indices, 'temperature_anomaly'].any())
    
    def test_cluster_detection(self):
        # Add farm_id for clustering
        self.test_data['farm_id'] = 'test_farm'
        self.test_data['tag_id'] = 'test_animal'
        self.test_data['animal_type'] = 'cattle'
        
        result = self.detector.detect_anomalies(self.test_data, ['temperature'])
        clusters = self.detector.detect_clusters(result, time_window='7D', min_cluster_size=1)
        
        self.assertIsInstance(clusters, list)

if __name__ == '__main__':
    unittest.main()