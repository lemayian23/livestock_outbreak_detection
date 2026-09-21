"""
Configuration loader — supports both attribute access and dict-style .get().

This class is the single source of truth for pipeline configuration.
It reads config/settings.yaml and exposes:

  - Attribute access:  config.anomaly_config.method
  - Dict-style access: config.get('anomaly_detection.method')
  - Raw dict:          config.raw_config
"""

import os
import yaml
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any


@dataclass
class DatabaseConfig:
    path: Optional[str] = None
    type: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    name: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    pool_size: Optional[int] = None
    timeout: Optional[int] = None


@dataclass
class AnomalyConfig:
    method: Optional[str] = None
    threshold: Optional[float] = None
    window_size: Optional[int] = None
    min_anomalies_for_alert: Optional[int] = None
    ensemble_weights: Optional[Dict[str, float]] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def __init__(self, **kwargs):
        declared = {
            'method', 'threshold', 'window_size',
            'min_anomalies_for_alert', 'ensemble_weights',
        }
        for key, value in kwargs.items():
            if key in declared:
                setattr(self, key, value)
            else:
                self.extra[key] = value


@dataclass
class MetricRanges:
    temperature: Dict[str, List[float]] = field(default_factory=dict)
    heart_rate: Dict[str, List[float]] = field(default_factory=dict)
    activity_level: List[float] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

    def __init__(self, **kwargs):
        declared = {'temperature', 'heart_rate', 'activity_level'}
        for key, value in kwargs.items():
            if key in declared:
                setattr(self, key, value)
            else:
                self.extra[key] = value


class Config:
    """
    Unified configuration object.

    Usage:
        cfg = Config()
        cfg.get('anomaly_detection.method')       # 'statistical'
        cfg.anomaly_config.method                 # 'statistical'
        cfg.raw_config['database']['host']        # 'localhost'
        cfg['app']['name']                        # 'livestock_outbreak_detection'
    """

    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(__file__),
                '..', '..', 'config', 'settings.yaml'
            )

        with open(config_path, 'r') as f:
            self.raw_config = yaml.safe_load(f) or {}

        # Structured sub-configs
        db_raw = self.raw_config.get('database', {}) or {}
        self.db_config = DatabaseConfig(**db_raw)

        anomaly_raw = self.raw_config.get('anomaly_detection', {}) or {}
        self.anomaly_config = AnomalyConfig(**anomaly_raw)

        # Expose isolation forest config as attribute (used by detector)
        self.isolation_forest = self.raw_config.get('isolation_forest', {}) or {}

        metrics_raw = (self.raw_config.get('livestock_metrics', {}) or {}).get(
            'normal_ranges', {}
        ) or {}
        self.metric_ranges = MetricRanges(**metrics_raw)

        # Expose features for feature_manager
        self.features = self.raw_config.get('features', {}) or {}

    # --- Dict-style interface ---------------------------------------------
    def get(self, key: str, default: Any = None) -> Any:
        """
        Dot-notation lookup that works against raw_config.

        Examples:
            get('anomaly_detection.method')
            get('database.host', 'localhost')
        """
        parts = key.split('.')
        value: Any = self.raw_config

        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return default
        return value

    def __getitem__(self, key):
        return self.raw_config[key]

    def __contains__(self, key):
        return key in self.raw_config

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.raw_config)

    # --- Metric range helper ----------------------------------------------
    def get_normal_range(self, metric: str, animal_type: str = None):
        ranges = getattr(self.metric_ranges, metric, None)
        if ranges and animal_type and isinstance(ranges, dict):
            return ranges.get(animal_type, [0, 1])
        return ranges