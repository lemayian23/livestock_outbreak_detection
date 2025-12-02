"""
Feature normalization and scaling
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import joblib
import os

class FeatureNormalizer:
    """Normalizes features for anomaly detection"""
    
    def __init__(self, method: str = 'standard'):
        """
        Initialize normalizer
        
        Args:
            method: 'standard' (z-score) or 'minmax' (0-1 scaling)
        """
        self.method = method
        self.scalers = {}
        self.fitted = False
    
    def fit(self, df: pd.DataFrame, feature_cols: List[str]):
        """Fit scalers to data"""
        for col in feature_cols:
            if col not in df.columns:
                continue
            
            if self.method == 'standard':
                scaler = StandardScaler()
            else:  # minmax
                scaler = MinMaxScaler()
            
            # Reshape for sklearn
            data = df[col].values.reshape(-1, 1)
            
            # Handle NaN values
            mask = ~np.isnan(data).flatten()
            if mask.any():
                scaler.fit(data[mask])
                self.scalers[col] = scaler
        
        self.fitted = True
    
    def transform(self, df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
        """Transform features using fitted scalers"""
        if not self.fitted:
            raise ValueError("Normalizer must be fitted before transformation")
        
        df_transformed = df.copy()
        
        for col in feature_cols:
            if col not in df.columns or col not in self.scalers:
                continue
            
            scaler = self.scalers[col]
            
            # Reshape for sklearn
            data = df[col].values.reshape(-1, 1)
            
            # Create mask for non-NaN values
            mask = ~np.isnan(data).flatten()
            
            if mask.any():
                # Transform non-NaN values
                transformed_data = np.full_like(data, np.nan)
                transformed_data[mask] = scaler.transform(data[mask])
                
                # Create new column name
                new_col = f"{col}_normalized"
                df_transformed[new_col] = transformed_data.flatten()
        
        return df_transformed
    
    def fit_transform(self, df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
        """Fit and transform in one step"""
        self.fit(df, feature_cols)
        return self.transform(df, feature_cols)
    
    def inverse_transform(self, df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
        """Inverse transform normalized features"""
        if not self.fitted:
            raise ValueError("Normalizer must be fitted before inverse transformation")
        
        df_inverse = df.copy()
        
        for col in feature_cols:
            normalized_col = f"{col}_normalized"
            
            if normalized_col not in df.columns or col not in self.scalers:
                continue
            
            scaler = self.scalers[col]
            
            # Reshape for sklearn
            data = df[normalized_col].values.reshape(-1, 1)
            
            # Create mask for non-NaN values
            mask = ~np.isnan(data).flatten()
            
            if mask.any():
                # Inverse transform non-NaN values
                inverse_data = np.full_like(data, np.nan)
                inverse_data[mask] = scaler.inverse_transform(data[mask])
                
                # Create column for inverse transformed values
                inverse_col = f"{col}_original"
                df_inverse[inverse_col] = inverse_data.flatten()
        
        return df_inverse
    
    def save(self, filepath: str):
        """Save fitted normalizer to disk"""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        joblib.dump({
            'method': self.method,
            'scalers': self.scalers,
            'fitted': self.fitted
        }, filepath)
    
    def load(self, filepath: str):
        """Load normalizer from disk"""
        data = joblib.load(filepath)
        self.method = data['method']
        self.scalers = data['scalers']
        self.fitted = data['fitted']