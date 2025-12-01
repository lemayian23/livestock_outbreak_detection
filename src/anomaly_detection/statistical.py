import numpy as np
import pandas as pd
from scipy import stats
from typing import Tuple, Dict, List

class StatisticalDetector:
    """Statistical anomaly detection using Z-scores and moving averages"""
    
    def __init__(self, window_size: int = 7, threshold: float = 3.0):
        self.window_size = window_size
        self.threshold = threshold
        
    def calculate_zscore(self, series: pd.Series) -> pd.Series:
        """Calculate rolling z-score"""
        rolling_mean = series.rolling(window=self.window_size, min_periods=1).mean()
        rolling_std = series.rolling(window=self.window_size, min_periods=1).std()
        
        # Avoid division by zero
        rolling_std = rolling_std.replace(0, np.nan)
        
        z_scores = (series - rolling_mean) / rolling_std
        return z_scores
    
    def detect_anomalies(self, df: pd.DataFrame, 
                        metric_cols: List[str]) -> pd.DataFrame:
        """Detect anomalies in multiple metrics"""
        
        result_df = df.copy()
        
        for col in metric_cols:
            if col not in df.columns:
                continue
                
            # Calculate z-scores
            z_scores = self.calculate_zscore(df[col])
            
            # Identify anomalies
            anomaly_mask = np.abs(z_scores) > self.threshold
            
            # Mark as anomaly
            result_df[f'{col}_zscore'] = z_scores
            result_df[f'{col}_anomaly'] = anomaly_mask
        
        # Combined anomaly flag
        anomaly_cols = [f'{col}_anomaly' for col in metric_cols if f'{col}_anomaly' in result_df.columns]
        if anomaly_cols:
            result_df['is_anomaly'] = result_df[anomaly_cols].any(axis=1)
            
            # Calculate combined anomaly score
            zscore_cols = [f'{col}_zscore' for col in metric_cols if f'{col}_zscore' in result_df.columns]
            if zscore_cols:
                result_df['anomaly_score'] = result_df[zscore_cols].abs().max(axis=1)
        
        return result_df
    
    def detect_clusters(self, df: pd.DataFrame, 
                       time_window: str = '7D',
                       min_cluster_size: int = 3) -> List[Dict]:
        """Detect clusters of anomalies in time and space"""
        
        if 'is_anomaly' not in df.columns:
            df = self.detect_anomalies(df, ['temperature', 'heart_rate', 'activity_level'])
        
        anomalies = df[df['is_anomaly']].copy()
        
        if len(animals) < min_cluster_size:
            return []
        
        # Group by farm and time window
        anomalies['time_window'] = pd.to_datetime(animals['date']).dt.floor(time_window)
        
        clusters = []
        for (farm_id, window), group in anomalies.groupby(['farm_id', 'time_window']):
            if len(group) >= min_cluster_size:
                cluster = {
                    'farm_id': farm_id,
                    'start_date': window,
                    'end_date': window + pd.Timedelta(time_window),
                    'affected_animals': len(group['tag_id'].unique()),
                    'avg_anomaly_score': group['anomaly_score'].mean(),
                    'animal_types': group['animal_type'].unique().tolist(),
                    'metrics_affected': self._get_affected_metrics(group)
                }
                clusters.append(cluster)
        
        return clusters
    
    def _get_affected_metrics(self, anomaly_group: pd.DataFrame) -> List[str]:
        """Identify which metrics are most affected in an anomaly group"""
        metrics = ['temperature', 'heart_rate', 'activity_level']
        affected = []
        
        for metric in metrics:
            anomaly_col = f'{metric}_anomaly'
            if anomaly_col in anomaly_group.columns and anomaly_group[anomaly_col].any():
                affected.append(metric)
        
        return affected