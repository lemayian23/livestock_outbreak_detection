#!/usr/bin/env python3
"""
Generate realistic livestock outbreak data aligned with the validation schema.
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
    anomaly_percentage: float = 0.05,
):
    """
    Generate realistic livestock health data with optional anomalies.

    Columns emitted (matching data_validation.schema daily_health_metrics):
        farm_id, date, animal_type, total_animals, sick_animals,
        deceased_animals, avg_temperature, feed_intake_percent,
        water_intake_percent, activity_level, location_lat, location_lon
    Extra columns (harmless, used by dashboard):
        tag_id
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    animal_types = ['cattle', 'swine', 'poultry', 'sheep', 'goat']
    farm_names = [f"FARM-{i:04d}" for i in range(1, num_farms + 1)]

    base_temperature = 38.5
    base_activity = 7.0
    base_feed_intake = 90.0
    base_water_intake = 85.0

    records = []
    anomalies = []

    start_date = datetime.now() - timedelta(days=num_days)

    for day in range(num_days):
        current_date = start_date + timedelta(days=day)
        date_str = current_date.strftime("%Y-%m-%d")

        for farm in farm_names:
            animal_type = random.choice(animal_types)
            total_animals = random.randint(50, 500)

            temp = base_temperature + random.gauss(0, 0.5)
            activity = base_activity + random.gauss(0, 0.5)
            feed = base_feed_intake + random.gauss(0, 5)
            water = base_water_intake + random.gauss(0, 5)

            sick = max(0, int(random.gauss(total_animals * 0.02, total_animals * 0.01)))
            deceased = max(0, int(random.gauss(sick * 0.3, sick * 0.1)))
            sick = min(sick, total_animals)
            deceased = min(deceased, sick)

            lat = random.uniform(-30, 5)
            lon = random.uniform(20, 45)

            record = {
                'farm_id': farm,
                'date': date_str,
                'tag_id': f"{farm}-{animal_type[:3].upper()}-HERD",
                'animal_type': animal_type,
                'total_animals': total_animals,
                'sick_animals': sick,
                'deceased_animals': deceased,
                'avg_temperature': round(temp, 2),
                'feed_intake_percent': round(feed, 2),
                'water_intake_percent': round(water, 2),
                'activity_level': round(activity, 2),
                'location_lat': round(lat, 4),
                'location_lon': round(lon, 4),
            }
            records.append(record)

    df = pd.DataFrame(records)

    if include_anomalies and len(df) > 0:
        anomaly_indices = random.sample(
            range(len(df)), max(1, int(len(df) * anomaly_percentage))
        )

        for idx in anomaly_indices:
            anomaly_type = random.choice(
                ['temperature_spike', 'sick_spike', 'feed_drop', 'activity_drop']
            )

            if anomaly_type == 'temperature_spike':
                df.loc[idx, 'avg_temperature'] = 42.0 + random.uniform(0, 2)
                severity = 'high'
                description = f"Temperature spike in {df.loc[idx, 'animal_type']} population"
            elif anomaly_type == 'sick_spike':
                spike = int(df.loc[idx, 'total_animals'] * random.uniform(0.15, 0.4))
                df.loc[idx, 'sick_animals'] = spike
                severity = 'high'
                description = f"Sudden sick spike in {df.loc[idx, 'animal_type']} farm"
            elif anomaly_type == 'feed_drop':
                df.loc[idx, 'feed_intake_percent'] = random.uniform(20, 40)
                severity = 'medium'
                description = "Feed intake dropped significantly"
            else:
                df.loc[idx, 'activity_level'] = random.uniform(1, 3)
                severity = 'medium'
                description = "Animal activity dropped unexpectedly"

            anomalies.append({
                'timestamp': df.loc[idx, 'date'],
                'date': df.loc[idx, 'date'],
                'farm_id': df.loc[idx, 'farm_id'],
                'animal_type': df.loc[idx, 'animal_type'],
                'severity': severity,
                'score': round(random.uniform(0.7, 0.98), 3),
                'description': description,
            })

    csv_path = output_path / "livestock_data.csv"
    df.to_csv(csv_path, index=False)
    print(f"✅ Data saved to {csv_path}")
    print(f"   {len(df)} records, {len(anomalies)} anomalies")

    anomalies_path = None
    if anomalies:
        anomalies_path = output_path / "anomalies.json"
        with open(anomalies_path, 'w') as f:
            json.dump(anomalies, f, indent=2, default=str)
        print(f"✅ Anomalies saved to {anomalies_path}")

    sample_path = output_path / "livestock_data_sample.csv"
    df.head(20).to_csv(sample_path, index=False)
    print(f"✅ Sample (20 rows) saved to {sample_path}")

    return csv_path, anomalies_path


def main():
    parser = argparse.ArgumentParser(description='Generate sample livestock data')
    parser.add_argument('--days', type=int, default=30)
    parser.add_argument('--farms', type=int, default=10)
    parser.add_argument('--output', default='data/raw')
    parser.add_argument('--no-anomalies', action='store_true')
    parser.add_argument('--anomaly-rate', type=float, default=0.05)

    args = parser.parse_args()

    generate_livestock_data(
        num_days=args.days,
        num_farms=args.farms,
        output_dir=args.output,
        include_anomalies=not args.no_anomalies,
        anomaly_percentage=args.anomaly_rate,
    )


if __name__ == "__main__":
    main()