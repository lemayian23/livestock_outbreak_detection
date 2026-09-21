"""
Main anomaly detection orchestrator
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from enum import Enum
from types import SimpleNamespace

from .statistical import StatisticalDetector
from .isolation_forest import IsolationForestDetector
from .ensemble import EnsembleDetector


class DetectionMethod(Enum):
    STATISTICAL = "statistical"
    ISOLATION_FOREST = "isolation_forest"
    ENSEMBLE = "ensemble"
    LSTM = "lstm"


class AnomalyDetector:
    """Orchestrates different anomaly detection methods"""

    def __init__(self, config):
        # Normalize config: accept either a Config object (with .anomaly_config
        # and .isolation_forest attributes) or a plain dict (from config_manager).
        config = self._normalize_config(config)
        self.config = config
        self.detector = None
        self._initialize_detector()

    @staticmethod
    def _normalize_config(config):
        """Return an object with .anomaly_config and .isolation_forest attributes."""
        if not isinstance(config, dict):
            return config  # already the legacy Config object

        anomaly_cfg = config.get('anomaly_detection', {}) or {}
        iso_cfg = config.get('isolation_forest', {}) or {}

        return SimpleNamespace(
            anomaly_config=SimpleNamespace(
                method=anomaly_cfg.get('method', 'statistical'),
                threshold=anomaly_cfg.get('threshold', 3.0),
                window_size=anomaly_cfg.get('window_size', 7),
                min_anomalies_for_alert=anomaly_cfg.get(
                    'min_anomalies_for_alert', 3
                ),
                ensemble_weights=anomaly_cfg.get(
                    'ensemble_weights',
                    {'statistical': 0.4, 'isolation_forest': 0.6}
                ),
            ),
            isolation_forest=iso_cfg,
        )

    def _initialize_detector(self):
        """Initialize the appropriate detector based on config"""
        method = self.config.anomaly_config.method

        if method == DetectionMethod.STATISTICAL.value:
            self.detector = StatisticalDetector(
                window_size=self.config.anomaly_config.window_size,
                threshold=self.config.anomaly_config.threshold,
            )
        elif method == DetectionMethod.ISOLATION_FOREST.value:
            self.detector = IsolationForestDetector(
                contamination=self.config.isolation_forest.get('contamination', 0.1),
                n_estimators=self.config.isolation_forest.get('n_estimators', 100),
                random_state=self.config.isolation_forest.get('random_state', 42),
            )
        elif method == DetectionMethod.ENSEMBLE.value:
            self.detector = EnsembleDetector(
                statistical_config={
                    'window_size': self.config.anomaly_config.window_size,
                    'threshold': self.config.anomaly_config.threshold,
                },
                isolation_forest_config={
                    'contamination': self.config.isolation_forest.get(
                        'contamination', 0.1
                    ),
                    'n_estimators': self.config.isolation_forest.get(
                        'n_estimators', 100
                    ),
                    'random_state': self.config.isolation_forest.get(
                        'random_state', 42
                    ),
                },
                weights=self.config.anomaly_config.ensemble_weights,
            )
        elif method == DetectionMethod.LSTM.value:
            from .lstm_detector import LSTMDetector
            self.detector = LSTMDetector(
                sequence_length=10,
                prediction_horizon=3,
            )
        else:
            raise ValueError(f"Unknown detection method: {method}")

    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect anomalies in health metrics.

        Args:
            df: DataFrame with health metrics.

        Returns:
            DataFrame with anomaly flags and scores.
        """
        metrics = ['temperature', 'heart_rate', 'activity_level']
        existing_metrics = [m for m in metrics if m in df.columns]

        if not existing_metrics:
            raise ValueError("No health metrics found in dataframe")

        result_df = self.detector.detect_anomalies(df, existing_metrics)
        return result_df

    def detect_outbreaks(
        self,
        df: pd.DataFrame,
        time_window: str = '7D',
        min_cluster_size: int = None,
    ) -> List[Dict]:
        """Detect outbreak clusters from anomalies."""
        if min_cluster_size is None:
            min_cluster_size = self.config.anomaly_config.min_anomalies_for_alert

        if hasattr(self.detector, 'detect_clusters'):
            clusters = self.detector.detect_clusters(
                df,
                time_window=time_window,
                min_cluster_size=min_cluster_size,
            )
        else:
            clusters = self._basic_cluster_detection(df, min_cluster_size)

        return clusters

    def _basic_cluster_detection(
        self, df: pd.DataFrame, min_cluster_size: int
    ) -> List[Dict]:
        """Basic cluster detection when detector doesn't have cluster method."""
        if 'is_anomaly' not in df.columns:
            df = self.detect(df)

        anomalies = df[df['is_anomaly']].copy()

        if len(anomalies) < min_cluster_size:
            return []

        anomalies['date_day'] = pd.to_datetime(anomalies['date']).dt.date

        clusters = []

        if 'farm_id' in df.columns:
            grouped = anomalies.groupby(['farm_id', 'date_day'])

            for (farm_id, date_day), group in grouped:
                if len(group) >= min_cluster_size:
                    cluster = {
                        'farm_id': farm_id,
                        'start_date': pd.Timestamp(date_day),
                        'end_date': pd.Timestamp(date_day),
                        'affected_animals': len(group['tag_id'].unique()),
                        'avg_anomaly_score': group['anomaly_score'].mean(),
                        'animal_types': (
                            group['animal_type'].unique().tolist()
                            if 'animal_type' in group.columns else []
                        ),
                        'detection_method': self.config.anomaly_config.method,
                    }

                    if hasattr(self.detector, 'feature_cols') and self.detector.feature_cols:
                        cluster['features_contributing'] = self.detector.feature_cols[:3]

                    clusters.append(cluster)

        return clusters