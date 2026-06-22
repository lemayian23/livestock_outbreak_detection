#!/usr/bin/env python3
"""
Generate realistic livestock outbreak data with anomalies
"""
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from pathlib import Path
import argparse
import json


def generate_livestock_data(
    num_days: int = 30,
    num_farms: int = 10,
    output_dir: str = "data/raw",
    include_anomalies: bool = True,
    anomaly_percentage: float = 0.05
):
    """
    Generate realistic livestock health data with optional anomalies
    
    Args:
        num_days: Number of days of data
        num_farms: Number of farms
        output_dir: Directory to save CSV files
        include_anomalies: Whether to inject anomalies
        anomaly_percentage: Percentage of records that should be anomalous
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Farm types
    animal_types = ['cattle', 'swine', 'poultry', 'sheep', 'goat']
    farm_names = [f"FARM-{i:04d}" for i in range(1, num_farms + 1)]
    
    # Base parameters
    base_temperature = 38.5  # Celsius
    base_activity = 7.0      # Scale 0-10
    base_feed_intake = 90.0  # Percentage
    base_water_intake = 85.0 # Percentage
    
    records = []
    anomalies = []
    
    start_date = datetime.now() - timedelta(days=num_days)
    
    for day in range(num_days):
        current_date = start_date + timedelta(days=day)
        
        for farm in farm_names:
            # Randomly assign animal type (some farms have multiple, but we'll keep it simple)
            animal_type = random.choice(animal_types)
            
            # Base values with daily variation
            total_animals = random.randint(50, 500)
            
            # Normal variation
            temp = base_temperature + random.gauss(0, 0.5)
            activity = base_activity + random.gauss(0, 0.5)
            feed = base_feed_intake + random.gauss(0, 5)
            water = base_water_intake + random.gauss(0, 5)
            
            # Sick and deceased counts (typically low)
            sick = max(0, int(random.gauss(total_animals * 0.02, total_animals * 0.01)))
            deceased = max(0, int(random.gauss(sick * 0.3, sick * 0.1)))
            
            # Ensure sick <= total and deceased <= sick
            sick = min(sick, total_animals)
            deceased = min(deceased, sick)
            
            # Location
            lat = random.uniform(-30, 5)   # Roughly Africa
            lon = random.uniform(20, 45)
            
            record = {
                'timestamp': current_date.strftime("%Y-%m-%d"),
                'farm_id': farm,
                'animal_type': animal_type,
                'total_animals': total_animals,
                'sick_animals': sick,
                'deceased_animals': deceased,
                'avg_temperature': round(temp, 2),
                'activity_level': round(activity, 2),
                'feed_intake_percent': round(feed, 2),
                'water_intake_percent': round(water, 2),
                'location_lat': round(lat, 4),
                'location_lon': round(lon, 4)
            }
            
            records.append(record)
    
    # Convert to DataFrame
    df = pd.DataFrame(records)
    
    # Inject anomalies if requested
    if include_anomalies:
        anomaly_indices = random.sample(range(len(df)), int(len(df) * anomaly_percentage))
        
        for idx in anomaly_indices:
            # Choose anomaly type
            anomaly_type = random.choice([
                'temperature_spike',
                'sick_spike',
                'feed_drop',
                'activity_drop'
            ])
            
            if anomaly_type == 'temperature_spike':
                df.loc[idx, 'avg_temperature'] = 42.0 + random.uniform(0, 2)
                severity = 'high'
                description = f"Temperature spike in {df.loc[idx, 'animal_type']} population"
            elif anomaly_type == 'sick_spike':
                sick_spike = int(df.loc[idx, 'total_animals'] * random.uniform(0.15, 0.4))
                df.loc[idx, 'sick_animals'] = sick_spike
                severity = 'high'
                description = f"Sudden sick spike in {df.loc[idx, 'animal_type']} farm"
            elif anomaly_type == 'feed_drop':
                df.loc[idx, 'feed_intake_percent'] = random.uniform(20, 40)
                severity = 'medium'
                description = f"Feed intake dropped significantly"
            else:  # activity_drop
                df.loc[idx, 'activity_level'] = random.uniform(1, 3)
                severity = 'medium'
                description = f"Animal activity dropped unexpectedly"
            
            # Add to anomalies list
            anomalies.append({
                'timestamp': df.loc[idx, 'timestamp'],
                'farm_id': df.loc[idx, 'farm_id'],
                'animal_type': df.loc[idx, 'animal_type'],
                'severity': severity,
                'score': round(random.uniform(0.7, 0.98), 3),
                'description': description
            })
    
    # Save data
    csv_path = output_path / "livestock_data.csv"
    df.to_csv(csv_path, index=False)
    print(f"✅ Data saved to {csv_path}")
    print(f"   {len(df)} records, {len(anomalies)} anomalies")
    
    # Save anomalies if any
    if anomalies:
        anomalies_path = output_path / "anomalies.json"
        with open(anomalies_path, 'w') as f:
            json.dump(anomalies, f, indent=2)
        print(f"✅ Anomalies saved to {anomalies_path}")
    
    # Also save a sample of the data for quick viewing
    sample_path = output_path / "livestock_data_sample.csv"
    df.head(20).to_csv(sample_path, index=False)
    print(f"✅ Sample (20 rows) saved to {sample_path}")
    
    return csv_path, anomalies_path if anomalies else None


def main():
    parser = argparse.ArgumentParser(description='Generate sample livestock data')
    parser.add_argument('--days', type=int, default=30, help='Number of days')
    parser.add_argument('--farms', type=int, default=10, help='Number of farms')
    parser.add_argument('--output', default='data/raw', help='Output directory')
    parser.add_argument('--no-anomalies', action='store_true', help='Skip anomalies')
    parser.add_argument('--anomaly-rate', type=float, default=0.05, 
                       help='Percentage of records to be anomalous (0-1)')
    
    args = parser.parse_args()
    
    generate_livestock_data(
        num_days=args.days,
        num_farms=args.farms,
        output_dir=args.output,
        include_anomalies=not args.no_anomalies,
        anomaly_percentage=args.anomaly_rate
    )


if __name__ == "__main__":
    main()