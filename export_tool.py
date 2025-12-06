#!/usr/bin/env python3
"""
Command-line tool for exporting data from the livestock system
"""

import sys
import os
import argparse
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.export.exporter import DataExporter
from src.database.models import DatabaseManager
import pandas as pd

def export_from_database(export_type, output_format, days_back):
    """Export data directly from database"""
    
    # Connect to database
    db_manager = DatabaseManager()
    
    with db_manager.get_session() as session:
        if export_type == 'anomalies':
            query = """
            SELECT * FROM health_metrics 
            WHERE is_anomaly = 1
            AND date >= date('now', '-' || :days || ' days')
            """
            df = pd.read_sql_query(query, session.bind, params={'days': days_back})
            
            if df.empty:
                print("No anomalies found in the specified time range.")
                return
            
            exporter = DataExporter()
            files = exporter.export_anomalies(df)
            
        elif export_type == 'alerts':
            query = """
            SELECT * FROM outbreak_alerts 
            WHERE created_at >= date('now', '-' || :days || ' days')
            """
            df = pd.read_sql_query(query, session.bind, params={'days': days_back})
            
            if df.empty:
                print("No alerts found in the specified time range.")
                return
            
            exporter = DataExporter()
            files = exporter.export_alerts(df)
            
        elif export_type == 'metrics':
            query = """
            SELECT * FROM health_metrics 
            WHERE date >= date('now', '-' || :days || ' days')
            """
            df = pd.read_sql_query(query, session.bind, params={'days': days_back})
            
            if df.empty:
                print("No health metrics found in the specified time range.")
                return
            
            exporter = DataExporter()
            files = exporter.export_health_metrics(df, f'last_{days_back}_days')
            
        elif export_type == 'summary':
            # Get data for summary
            anomalies_query = """
            SELECT * FROM health_metrics 
            WHERE date >= date('now', '-' || :days || ' days')
            """
            anomalies_df = pd.read_sql_query(anomalies_query, session.bind, params={'days': days_back})
            
            alerts_query = """
            SELECT * FROM outbreak_alerts 
            WHERE created_at >= date('now', '-' || :days || ' days')
            """
            alerts_df = pd.read_sql_query(alerts_query, session.bind, params={'days': days_back})
            alerts = alerts_df.to_dict('records')
            
            exporter = DataExporter()
            file = exporter.generate_summary_report(
                anomalies_df, 
                alerts, 
                output_format=output_format
            )
            
            print(f"Summary report generated: {file}")
            return
            
        else:
            print(f"Unknown export type: {export_type}")
            return
    
    # Show results
    if files:
        print(f"\nExported {len(df)} {export_type} to:")
        for fmt, filepath in files.items():
            print(f"  {fmt.upper()}: {filepath}")
    else:
        print("Export failed or no data to export.")

def list_exports(days=7):
    """List available exports"""
    exporter = DataExporter()
    exports = exporter.list_exports(days)
    
    if exports:
        print(f"\nFound {len(exports)} export files from last {days} days:")
        print("-" * 80)
        for exp in exports:
            print(f"{exp['filename']} ({exp['size_human']}) - {exp['modified']}")
    else:
        print(f"No export files found from last {days} days.")

def main():
    parser = argparse.ArgumentParser(description='Export data from Livestock Outbreak Detection System')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export data')
    export_parser.add_argument('type', choices=['anomalies', 'alerts', 'metrics', 'summary'],
                              help='Type of data to export')
    export_parser.add_argument('--format', choices=['csv', 'excel', 'json', 'txt', 'md'],
                              default='csv', help='Output format')
    export_parser.add_argument('--days', type=int, default=30,
                              help='Number of days to look back')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List exports')
    list_parser.add_argument('--days', type=int, default=7,
                           help='Number of days to look back')
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Cleanup old exports')
    cleanup_parser.add_argument('--days', type=int, default=30,
                              help='Keep files newer than this many days')
    
    args = parser.parse_args()
    
    if args.command == 'export':
        export_from_database(args.type, args.format, args.days)
        
    elif args.command == 'list':
        list_exports(args.days)
        
    elif args.command == 'cleanup':
        exporter = DataExporter()
        deleted = exporter.cleanup_old_exports(args.days)
        print(f"Deleted {deleted} old export files.")
        
    else:
        parser.print_help()

if __name__ == "__main__":
    main()