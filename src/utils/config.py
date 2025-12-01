import yaml
import os
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class DatabaseConfig:
    path: str
    type: str

@dataclass
class AnomalyConfig:
    method: str
    threshold: float
    window_size: int
    min_anomalies_for_alert: int

@dataclass
class MetricRanges:
    temperature: Dict[str, List[float]]
    heart_rate: Dict[str, List[float]]
    activity_level: List[float]

class Config:
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(__file__), 
                '../../config/settings.yaml'
            )
        
        with open(config_path, 'r') as f:
            self.raw_config = yaml.safe_load(f)
        
        # Parse configurations
        self.db_config = DatabaseConfig(**self.raw_config['database'])
        self.anomaly_config = AnomalyConfig(**self.raw_config['anomaly_detection'])
        self.metric_ranges = MetricRanges(**self.raw_config['livestock_metrics']['normal_ranges'])
    
    def get_normal_range(self, metric: str, animal_type: str = None):
        """Get normal range for a specific metric and animal type"""
        ranges = getattr(self.metric_ranges, metric, None)
        if ranges and animal_type:
            return ranges.get(animal_type, [0, 1])
        return ranges