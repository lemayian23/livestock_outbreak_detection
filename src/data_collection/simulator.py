import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List
import random

class DataSimulator:
    """Simulates livestock health data for testing"""
    
    def __init__(self, config):
        self.config = config
        self.animal_types = ['cattle', 'sheep', 'goat']
        self.farm_ids = ['farm_001', 'farm_002', 'farm_003']
        
    def generate_normal_metric(self, metric: str, animal_type: str) -> float:
        """Generate a normal health metric value"""
        normal_range = self.config.get_normal_range(metric, animal_type)
        
        if metric == 'temperature':
            return np.random.normal(
                loc=sum(normal_range)/2,
                scale=(normal_range[1] - normal_range[0])/6
            )
        elif metric == 'heart_rate':
            return np.random.normal(
                loc=sum(normal_range)/2,
                scale=(normal_range[1] - normal_range[0])/4
            )
        elif metric == 'activity_level':
            return np.random.normal(loc=1.0, scale=0.2)
        else:
            return np.random.random()
    
    def inject_outbreak(self, metrics_df: pd.DataFrame, 
                       start_date: datetime, 
                       duration_days: int = 5,
                       affected_percentage: float = 0.3) -> pd.DataFrame:
        """Inject synthetic outbreak patterns into data"""
        
        outbreak_data = metrics_df.copy()
        end_date = start_date + timedelta(days=duration_days)
        
        # Select random animals to be affected
        unique_animals = outbreak_data['tag_id'].unique()
        n_affected = int(len(unique_animals) * affected_percentage)
        affected_animals = np.random.choice(unique_animals, n_affected, replace=False)
        
        # Create outbreak pattern
        for animal in affected_animals:
            mask = (outbreak_data['tag_id'] == animal) & \
                   (outbreak_data['date'] >= start_date) & \
                   (outbreak_data['date'] <= end_date)
            
            # Increase temperature
            outbreak_data.loc[mask, 'temperature'] *= 1.1
            
            # Decrease activity
            outbreak_data.loc[mask, 'activity_level'] *= 0.7
            
            # Increase heart rate
            outbreak_data.loc[mask, 'heart_rate'] *= 1.15
            
            # Randomly add some missing data (simulating sick animals not eating)
            for idx in outbreak_data[mask].index:
                if random.random() < 0.3:
                    outbreak_data.loc[idx, 'feed_intake'] = np.nan
        
        return outbreak_data
    
    def generate_test_data(self, n_animals: int = 50, n_days: int = 90) -> pd.DataFrame:
        """Generate complete test dataset"""
        
        animals = []
        for i in range(n_animals):
            animals.append({
                'tag_id': f'ANM{str(i+1).zfill(4)}',
                'animal_type': np.random.choice(self.animal_types),
                'age_months': np.random.randint(6, 120),
                'farm_id': np.random.choice(self.farm_ids)
            })
        
        # Generate daily metrics
        all_metrics = []
        base_date = datetime.now() - timedelta(days=n_days)
        
        for day in range(n_days):
            current_date = base_date + timedelta(days=day)
            
            for animal in animals:
                metrics = {
                    'tag_id': animal['tag_id'],
                    'date': current_date,
                    'animal_type': animal['animal_type'],
                    'farm_id': animal['farm_id']
                }
                
                # Generate normal metrics
                for metric in ['temperature', 'heart_rate', 'activity_level']:
                    metrics[metric] = self.generate_normal_metric(
                        metric, animal['animal_type']
                    )
                
                # Generate intake metrics
                metrics['feed_intake'] = np.random.normal(loc=10, scale=2)
                metrics['water_intake'] = np.random.normal(loc=30, scale=5)
                
                all_metrics.append(metrics)
        
        df = pd.DataFrame(all_metrics)
        
        # Inject an outbreak
        outbreak_start = base_date + timedelta(days=60)
        df = self.inject_outbreak(df, outbreak_start, duration_days=7)
        
        return df