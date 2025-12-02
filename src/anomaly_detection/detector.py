"""
Main anomaly detection orchestrator
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from enum import Enum

from .statistical import StatisticalDetector
from src.utils.config import Config

class DetectionMethod(Enum):
    STATISTICAL = "statistical"
    ISOLATION_FOREST = "isolation_forest"
    LSTM = "lstm"

class AnomalyDetector:
    """Orchestrates different anomaly detection methods"""
    
    def __init__(self, config: Config):
        self.config = config
        self.detector = None
        self._initialize_detector()
    
    def _initialize_detector(self):
        """Initialize the appropriate detector based on config"""
        method = self.config.anomaly_config.method
        
        if method == DetectionMethod.STATISTICAL.value:
            self.detector = StatisticalDetector(
                window_size=self.config.anomaly_config.window_size,
                threshold=self.config.anomaly_config.threshold
            )
        elif method == DetectionMethod.ISOLATION_FOREST.value:
            # Placeholder for Isolation Forest implementation
            from .isolation_forest import IsolationForestDetector
            self.detector = IsolationForestDetector(
                contamination=0.1
            )
        elif method == DetectionMethod.LSTM.value:
            # Placeholder for LSTM implementation
            from .lstm_detector import LSTMDetector
            self.detector = LSTMDetector(
                sequence_length=10,
                prediction_horizon=3
            )
        else:
            raise ValueError(f"Unknown detection method: {method}")
    
    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect anomalies in health metrics
        
        Args:
            df: DataFrame with health metrics
            
        Returns:
            DataFrame with anomaly flags and scores
        """
        # Define metrics to monitor
        metrics = ['temperature', 'heart_rate', 'activity_level']
        
        # Ensure required columns exist
        existing_metrics = [m for m in metrics if m in df.columns]
        
        if not existing_metrics:
            raise ValueError("No health metrics found in dataframe")
        
        # Run detection
        result_df = self.detector.detect_anomalies(df, existing_metrics)
        
        return result_df
    
    def detect_outbreaks(self, df: pd.DataFrame, 
                        time_window: str = '7D',
                        min_cluster_size: int = None) -> List[Dict]:
        """
        Detect outbreak clusters from anomalies
        
        Args:
            df: DataFrame with anomaly flags
            time_window: Time window for clustering
            min_cluster_size: Minimum cluster size (uses config if None)
            
        Returns:
            List of outbreak clusters
        """
        if min_cluster_size is None:
            min_cluster_size = self.config.anomaly_config.min_anomalies_for_alert
        
        # Check if detector supports cluster detection
        if hasattr(self.detector, 'detect_clusters'):
            clusters = self.detector.detect_clusters(
                df, 
                time_window=time_window,
                min_cluster_size=min_cluster_size
            )
        else:
            # Fallback to basic clustering
            clusters = self._basic_cluster_detection(df, min_cluster_size)
        
        return clusters
    
    def _basic_cluster_detection(self, df: pd.DataFrame, 
                                min_cluster_size: int) -> List[Dict]:
        """Basic cluster detection when detector doesn't have cluster method"""
        
        if 'is_anomaly' not in df.columns:
            df = self.detect(df)
        
        anomalies = df[df['is_anomaly']].copy()
        
        if len(anomalies) < min_cluster_size:
            return []
        
        # Group by farm and date
        anomalies['date_day'] = pd.to_datetime(animals['date']).dt.date
        
        clusters = []
        
        if 'farm_id' in df.columns:
            # Group by farm and day
            grouped = anomalies.groupby(['farm_id', 'date_day'])
            
            for (farm_id, date_day), group in grouped:
                if len(group) >= min_cluster_size:
                    cluster = {
                        'farm_id': farm_id,
                        'start_date': pd.Timestamp(date_day),
                        'end_date': pd.Timestamp(date_day),
                        'affected_animals': len(group['tag_id'].unique()),
                        'avg_anomaly_score': group['anomaly_score'].mean(),
                        'animal_types': group['animal_type'].unique().tolist() 
                                      if 'animal_type' in group.columns else [],
                        'metrics_affected': self._identify_affected_metrics(group)
                    }
                    clusters.append(cluster)
        
        return clusters
    
    def _identify_affected_metrics(self, anomaly_group: pd.DataFrame) -> List[str]:
        """Identify which metrics are affected in anomaly group"""
        metrics = ['temperature', 'heart_rate', 'activity_level']
        affected = []
        
        for metric in metrics:
            anomaly_col = f'{metric}_anomaly'
            if anomaly_col in anomaly_group.columns and anomaly_group[anomaly_col].any():
                affected.append(metric)
            elif metric in anomaly_group.columns:
                # Check if values are outside normal range
                if 'animal_type' in anomaly_group.columns:
                    # This would need config-based range checking
                    affected.append(metric)
        
        return affected