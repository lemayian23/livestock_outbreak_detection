"""
Data cleaning and validation
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from src.utils.config import Config

class DataCleaner:
    """Cleans and validates livestock health data"""
    
    def __init__(self, config: Config):
        self.config = config
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all cleaning steps"""
        df = df.copy()
        
        # 1. Handle missing values
        df = self.handle_missing_values(df)
        
        # 2. Remove duplicates
        df = self.remove_duplicates(df)
        
        # 3. Fix data types
        df = self.fix_data_types(df)
        
        # 4. Validate ranges
        df = self.validate_ranges(df)
        
        # 5. Sort by date
        if 'date' in df.columns:
            df = df.sort_values('date')
        
        return df
    
    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values appropriately"""
        df = df.copy()
        
        # Fill numeric columns with forward fill, then backward fill
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            if col in ['temperature', 'heart_rate', 'activity_level']:
                # For health metrics, use forward fill within each animal
                if 'tag_id' in df.columns:
                    df[col] = df.groupby('tag_id')[col].transform(
                        lambda x: x.fillna(method='ffill').fillna(method='bfill')
                    )
                else:
                    df[col] = df[col].fillna(method='ffill').fillna(method='bfill')
        
        # Fill remaining numeric NaNs with column mean
        for col in numeric_cols:
            df[col] = df[col].fillna(df[col].mean())
        
        return df
    
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate records"""
        # Identify duplicates by key columns
        key_cols = ['tag_id', 'date']
        key_cols = [col for col in key_cols if col in df.columns]
        
        if key_cols:
            # Keep first occurrence of duplicates
            df = df.drop_duplicates(subset=key_cols, keep='first')
        
        return df
    
    def fix_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure correct data types"""
        df = df.copy()
        
        # Convert date column to datetime
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # Ensure numeric columns are float
        numeric_cols = ['temperature', 'heart_rate', 'activity_level', 
                       'feed_intake', 'water_intake']
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def validate_ranges(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate that values are within reasonable ranges"""
        df = df.copy()
        
        # Temperature validation
        if 'temperature' in df.columns and 'animal_type' in df.columns:
            for idx, row in df.iterrows():
                if pd.notna(row['temperature']) and pd.notna(row['animal_type']):
                    normal_range = self.config.get_normal_range('temperature', row['animal_type'])
                    if normal_range and not (normal_range[0] <= row['temperature'] <= normal_range[1]):
                        # Flag extreme values but don't remove
                        df.at[idx, 'temperature_flag'] = 'extreme'
        
        # Heart rate validation
        if 'heart_rate' in df.columns and 'animal_type' in df.columns:
            for idx, row in df.iterrows():
                if pd.notna(row['heart_rate']) and pd.notna(row['animal_type']):
                    normal_range = self.config.get_normal_range('heart_rate', row['animal_type'])
                    if normal_range and not (normal_range[0] <= row['heart_rate'] <= normal_range[1]):
                        df.at[idx, 'heart_rate_flag'] = 'extreme'
        
        # Activity level validation (should be positive)
        if 'activity_level' in df.columns:
            df['activity_level'] = df['activity_level'].clip(lower=0.1)
        
        return df
    
    def detect_outliers_iqr(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Detect outliers using IQR method"""
        if column not in df.columns:
            return df
        
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
        
        return outliers