#!/usr/bin/env python3
"""
Report generation CLI tool
"""
import argparse
import sys
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import glob

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from reporting.generator import get_report_generator


def find_csv_files():
    """Find all CSV files in common locations"""
    search_paths = [
        '.',                     # current directory
        'data/',
        'data/raw/',
        'data/processed/',
        'outputs/',
    ]
    csv_files = []
    for path in search_paths:
        pattern = os.path.join(path, '*.csv')
        csv_files.extend(glob.glob(pattern))
    return csv_files


def main():
    parser = argparse.ArgumentParser(
        description='Generate HTML reports from anomaly data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s sample --count 200
  %(prog)s from-csv data/raw/livestock.csv --title "Daily Report"
  %(prog)s from-csv data.csv --anomalies anomalies.json
  %(prog)s list
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # Generate report from CSV
    csv_parser = subparsers.add_parser('from-csv', help='Generate report from CSV file')
    csv_parser.add_argument('csv_file', help='Path to CSV file')
    csv_parser.add_argument('--anomalies', help='Path to anomalies JSON file (optional)')
    csv_parser.add_argument('--title', default='Anomaly Detection Report', help='Report title')
    csv_parser.add_argument('--output', help='Output file (optional)')
    
    # Generate sample report
    sample_parser = subparsers.add_parser('sample', help='Generate a sample report for testing')
    sample_parser.add_argument('--count', type=int, default=100, help='Number of sample records')
    
    # List reports
    subparsers.add_parser('list', help='List generated reports')
    
    # New: Show available CSV files
    subparsers.add_parser('find-csv', help='Show available CSV files in the project')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    generator = get_report_generator()
    
    if args.command == 'from-csv':
        # Check if CSV file exists
        if not os.path.exists(args.csv_file):
            print(f"❌ File not found: {args.csv_file}")
            print("\nAvailable CSV files in this project:")
            csv_files = find_csv_files()
            if csv_files:
                for f in csv_files:
                    print(f"  {f}")
            else:
                print("  No CSV files found.")
            print("\nPlease provide the correct path to your CSV file.")
            sys.exit(1)
        
        print(f"Reading data from {args.csv_file}...")
        try:
            df = pd.read_csv(args.csv_file)
        except Exception as e:
            print(f"❌ Failed to read CSV: {e}")
            sys.exit(1)
        
        # Load anomalies if provided
        anomalies = []
        if args.anomalies:
            if not os.path.exists(args.anomalies):
                print(f"⚠️  Anomalies file not found: {args.anomalies}")
                print("Continuing without anomalies data.")
            else:
                try:
                    with open(args.anomalies, 'r') as f:
                        anomalies = json.load(f)
                except Exception as e:
                    print(f"⚠️  Failed to load anomalies: {e}")
                    print("Continuing without anomalies data.")
        
        # Prepare metadata
        metadata = {
            'Source File': args.csv_file,
            'Total Rows': len(df),
            'Columns': ', '.join(df.columns[:5]) + ('...' if len(df.columns) > 5 else ''),
            'Generated': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        report_path = generator.generate_html_report(
            data=df,
            anomalies=anomalies,
            metadata=metadata,
            title=args.title
        )
        
        print(f"✅ Report generated: {report_path}")
        
    elif args.command == 'sample':
        print(f"Generating sample report with {args.count} records...")
        
        # Create sample data
        import random
        import numpy as np
        
        dates = [datetime.now() - timedelta(days=i) for i in range(args.count)]
        farms = [f"FARM-{random.randint(1,10):04d}" for _ in range(args.count)]
        animals = ['cattle', 'swine', 'poultry', 'sheep', 'goat']
        types = [random.choice(animals) for _ in range(args.count)]
        temperatures = [38.5 + random.uniform(-2, 2) for _ in range(args.count)]
        sick = [random.randint(0, 10) for _ in range(args.count)]
        total = [random.randint(50, 200) for _ in range(args.count)]
        
        df = pd.DataFrame({
            'timestamp': dates,
            'farm_id': farms,
            'animal_type': types,
            'temperature': temperatures,
            'sick_animals': sick,
            'total_animals': total
        })
        
        # Create sample anomalies (10% of data)
        anomalies = []
        anomaly_indices = random.sample(range(args.count), args.count // 10)
        for idx in anomaly_indices:
            anomalies.append({
                'timestamp': str(dates[idx]),
                'farm_id': farms[idx],
                'animal_type': types[idx],
                'severity': random.choice(['low', 'medium', 'high']),
                'score': random.uniform(0.7, 1.0),
                'description': f"Unusual pattern detected in {types[idx]} population"
            })
        
        metadata = {
            'Report Type': 'Sample Test',
            'Generated By': 'report_tool.py',
            'Sample Size': args.count
        }
        
        report_path = generator.generate_html_report(
            data=df,
            anomalies=anomalies,
            metadata=metadata,
            title="Sample Anomaly Detection Report"
        )
        
        print(f"✅ Sample report generated: {report_path}")
        
    elif args.command == 'list':
        reports_dir = Path("outputs/reports")
        if reports_dir.exists():
            reports = list(reports_dir.glob("*.html"))
            if reports:
                print(f"\n📊 Generated Reports ({len(reports)}):")
                for report in sorted(reports, key=lambda x: x.stat().st_mtime, reverse=True):
                    size = report.stat().st_size / 1024  # KB
                    modified = datetime.fromtimestamp(report.stat().st_mtime)
                    print(f"  {report.name} ({size:.1f} KB) - {modified.strftime('%Y-%m-%d %H:%M')}")
            else:
                print("No reports found.")
        else:
            print("No reports found.")
    
    elif args.command == 'find-csv':
        csv_files = find_csv_files()
        if csv_files:
            print("\n📄 Available CSV files:")
            for f in csv_files:
                size = os.path.getsize(f) / 1024
                print(f"  {f} ({size:.1f} KB)")
        else:
            print("No CSV files found. Try checking the data/ directory.")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()