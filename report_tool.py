#!/usr/bin/env python3
"""
Report generation CLI tool
"""
import argparse
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from reporting.generator import get_report_generator


def main():
    parser = argparse.ArgumentParser(description='Generate reports from anomaly data')

    subparsers = parser.add_subparsers(dest='command', help='Command')

    # Generate report from CSV
    csv_parser = subparsers.add_parser('from-csv', help='Generate report from CSV file')
    csv_parser.add_argument('csv_file', help='Path to CSV file')
    csv_parse.add_argument('--anomalies', help='Path to anomalies JSON file')
    csv_parser.add_argumant('--title', default='Anomaly Detection Report', help='Report title')
    csv_parser.add_argument('--output', help='Output file (optional)')

    # Generate sample report
    sample_parser = subparsers.add_parser('sample', help='Generate a sample repor for testing')
    sample_parser.add_argument('--count', type=int, default=100, help='Number of sample records')