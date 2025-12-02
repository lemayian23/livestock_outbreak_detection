"""
Data ingestion from various sources
"""

import pandas as pd
import json
import csv
from datetime import datetime
from typing import Union, List, Dict
import os

class DataIngestor:
    """Handles data ingestion from multiple sources"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def from_csv(self, file_path: str) -> pd.DataFrame:
        """Read data from CSV file"""
        try:
            df = pd.read_csv(file_path)
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            return df
        except Exception as e:
            raise ValueError(f"Error reading CSV file: {str(e)}")
    
    def from_json(self, file_path: str) -> pd.DataFrame:
        """Read data from JSON file"""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            if isinstance(data, dict):
                # Convert dict to list of records
                data = [data]
            
            df = pd.DataFrame(data)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
            
            return df
        except Exception as e:
            raise ValueError(f"Error reading JSON file: {str(e)}")
    
    def from_excel(self, file_path: str) -> pd.DataFrame:
        """Read data from Excel file"""
        try:
            df = pd.read_excel(file_path)
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            return df
        except Exception as e:
            raise ValueError(f"Error reading Excel file: {str(e)}")
    
    def validate_data(self, df: pd.DataFrame, required_cols: List[str]) -> bool:
        """Validate ingested data"""
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Check for required columns
        if 'tag_id' not in df.columns:
            raise ValueError("Missing 'tag_id' column")
        
        if 'date' not in df.columns:
            raise ValueError("Missing 'date' column")
        
        # Check for null values in critical columns
        critical_cols = ['tag_id', 'date']
        for col in critical_cols:
            if df[col].isnull().any():
                raise ValueError(f"Null values found in '{col}' column")
        
        return True