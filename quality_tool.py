#!/usr/bin/env python3
"""
Command-line tool for data quality analysis
"""

import sys
import os
import argparse
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.data_quality.analyzer import DataQualityAnalyzer
from src.database.models import DatabaseManager
import pandas as pd

def analyze_database(days=30, output_format='text'):
    """Analyze data quality from database"""
    
    # Connect to database
    db_manager = DatabaseManager()
    
    with db_manager.get_session() as session:
        # Get data
        query = f"""
        SELECT * FROM health_metrics 
        WHERE date >= date('now', '-' || :days || ' days')
        """
        
        df = pd.read_sql_query(query, session.bind, params={'days': days})
    
    if df.empty:
        print(f"No data found from the last {days} days.")
        return
    
    print(f"Analyzing {len(df)} records from last {days} days...")
    
    # Analyze
    analyzer = DataQualityAnalyzer()
    analysis = analyzer.analyze_dataframe(df)
    
    # Generate report
    report = analyzer.generate_quality_report(analysis, output_format)
    
    if output_format == 'text':
        print("\n" + "=" * 60)
        print(report)
        
        # Save to file
        save = input("\nSave report to file? (y/n): ").lower().strip()
        if save == 'y':
            timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
            filename = f'quality_report_{timestamp}.txt'
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"Report saved to: {filename}")
    
    elif output_format == 'html':
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        filename = f'quality_report_{timestamp}.html'
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"HTML report saved to: {filename}")
        print("Open this file in a web browser to view the report.")
    
    return analysis

def check_specific_issues(days=30):
    """Check for specific data quality issues"""
    
    db_manager = DatabaseManager()
    
    with db_manager.get_session() as session:
        query = f"""
        SELECT * FROM health_metrics 
        WHERE date >= date('now', '-' || :days || ' days')
        """
        
        df = pd.read_sql_query(query, session.bind, params={'days': days})
    
    if df.empty:
        print(f"No data found from the last {days} days.")
        return
    
    print(f"\nChecking {len(df)} records for issues...")
    print("=" * 60)
    
    # Check for missing values
    print("\n1. Missing Values:")
    print("-" * 40)
    
    for column in df.columns:
        missing = df[column].isna().sum()
        if missing > 0:
            percent = (missing / len(df)) * 100
            print(f"   {column}: {missing} missing ({percent:.1f}%)")
    
    # Check for duplicates
    print("\n2. Duplicate Records:")
    print("-" * 40)
    
    if 'tag_id' in df.columns and 'date' in df.columns:
        duplicates = df.duplicated(subset=['tag_id', 'date'], keep=False).sum()
        if duplicates > 0:
            percent = (duplicates / len(df)) * 100
            print(f"   Found {duplicates} duplicate records ({percent:.1f}%)")
        else:
            print("   No duplicates found")
    
    # Check for invalid values
    print("\n3. Invalid Values:")
    print("-" * 40)
    
    # Temperature checks
    if 'temperature' in df.columns:
        valid_temp = df['temperature'].between(35, 42)
        invalid_temp = (~valid_temp & df['temperature'].notna()).sum()
        if invalid_temp > 0:
            print(f"   Temperature: {invalid_temp} values outside 35-42°C range")
    
    # Heart rate checks
    if 'heart_rate' in df.columns:
        valid_hr = df['heart_rate'].between(40, 120)
        invalid_hr = (~valid_hr & df['heart_rate'].notna()).sum()
        if invalid_hr > 0:
            print(f"   Heart Rate: {invalid_hr} values outside 40-120 BPM range")
    
    # Check data freshness
    print("\n4. Data Freshness:")
    print("-" * 40)
    
    if 'date' in df.columns:
        try:
            df['date'] = pd.to_datetime(df['date'])
            latest = df['date'].max()
            today = pd.Timestamp.now()
            days_old = (today - latest).days
            
            print(f"   Latest data: {latest.strftime('%Y-%m-%d')}")
            print(f"   Data is {days_old} days old")
            
            if days_old > 7:
                print("   ⚠️  Data is getting stale")
        except:
            print("   Could not analyze dates")
    
    print("\n" + "=" * 60)

def main():
    parser = argparse.ArgumentParser(description='Data Quality Analysis Tool')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze data quality')
    analyze_parser.add_argument('--days', type=int, default=30,
                               help='Number of days to analyze')
    analyze_parser.add_argument('--format', choices=['text', 'html'],
                              default='text', help='Output format')
    
    # Issues command
    issues_parser = subparsers.add_parser('issues', help='Check for specific issues')
    issues_parser.add_argument('--days', type=int, default=30,
                             help='Number of days to check')
    
    # Score command
    score_parser = subparsers.add_parser('score', help='Get quality score')
    score_parser.add_argument('--days', type=int, default=30,
                            help='Number of days to analyze')
    
    args = parser.parse_args()
    
    if args.command == 'analyze':
        analyze_database(args.days, args.format)
        
    elif args.command == 'issues':
        check_specific_issues(args.days)
        
    elif args.command == 'score':
        db_manager = DatabaseManager()
        
        with db_manager.get_session() as session:
            query = f"""
            SELECT * FROM health_metrics 
            WHERE date >= date('now', '-' || :days || ' days')
            """
            
            df = pd.read_sql_query(query, session.bind, params={'days': args.days})
        
        if df.empty:
            print(f"No data found from the last {args.days} days.")
        else:
            analyzer = DataQualityAnalyzer()
            analysis = analyzer.analyze_dataframe(df)
            score = analysis.get('quality_score', 0)
            
            print(f"\nData Quality Score: {score:.1f}%")
            
            if score >= 90:
                print("Rating: ★★★★★ EXCELLENT")
            elif score >= 70:
                print("Rating: ★★★★☆ GOOD")
            elif score >= 50:
                print("Rating: ★★★☆☆ FAIR")
            elif score >= 30:
                print("Rating: ★★☆☆☆ POOR")
            else:
                print("Rating: ★☆☆☆☆ VERY POOR")
        
    else:
        parser.print_help()

if __name__ == "__main__":
    main()