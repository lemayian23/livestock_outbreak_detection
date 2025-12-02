"""
Seasonal pattern handling and decomposition
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from statsmodels.tsa.seasonal import seasonal_decompose
import warnings
warnings.filterwarnings('ignore')

class SeasonalHandler:
    """Handles seasonal patterns in livestock health data"""
    
    def __init__(self, period: int = 7):
        """
        Initialize seasonal handler
        
        Args:
            period: Seasonal period (e.g., 7 for weekly patterns)
        """
        self.period = period
    
    def decompose(self, series: pd.Series, model: str = 'additive') -> Dict:
        """
        Decompose time series into trend, seasonal, and residual components
        
        Args:
            series: Time series data
            model: 'additive' or 'multiplicative'
            
        Returns:
            Dictionary with decomposition results
        """
        # Ensure series is regular
        series = series.asfreq('D').fillna(method='ffill').fillna(method='bfill')
        
        # Decompose
        decomposition = seasonal_decompose(
            series, 
            model=model, 
            period=self.period,
            extrapolate_trend='freq'
        )
        
        return {
            'observed': decomposition.observed,
            'trend': decomposition.trend,
            'seasonal': decomposition.seasonal,
            'residual': decomposition.resid
        }
    
    def adjust_for_seasonality(self, df: pd.DataFrame, 
                              value_col: str,
                              group_col: str = None) -> pd.DataFrame:
        """
        Adjust values for seasonal patterns
        
        Args:
            df: DataFrame with time series data
            value_col: Column to adjust
            group_col: Column to group by (e.g., animal_id)
            
        Returns:
            DataFrame with seasonally adjusted values
        """
        df = df.copy()
        
        if group_col is None:
            # Simple seasonal adjustment for entire dataset
            series = df.set_index('date')[value_col]
            decomposition = self.decompose(series)
            
            # Remove seasonal component
            adjusted = decomposition['observed'] - decomposition['seasonal']
            df[f'{value_col}_seasonal_adj'] = adjusted.values
        
        else:
            # Group-wise seasonal adjustment
            adjusted_values = []
            
            for group, group_df in df.groupby(group_col):
                if len(group_df) < self.period * 2:
                    # Not enough data for seasonal decomposition
                    adjusted = group_df[value_col].values
                else:
                    series = group_df.set_index('date')[value_col]
                    decomposition = self.decompose(series)
                    
                    # Remove seasonal component
                    adjusted = decomposition['observed'] - decomposition['seasonal']
                
                group_df = group_df.copy()
                group_df[f'{value_col}_seasonal_adj'] = adjusted.values
                adjusted_values.append(group_df)
            
            df = pd.concat(adjusted_values, ignore_index=True)
        
        return df
    
    def detect_seasonal_anomalies(self, df: pd.DataFrame,
                                 value_col: str,
                                 threshold: float = 3.0) -> pd.DataFrame:
        """
        Detect anomalies in residuals after removing seasonal patterns
        
        Args:
            df: DataFrame with time series data
            value_col: Column to analyze
            threshold: Z-score threshold for residuals
            
        Returns:
            DataFrame with seasonal anomaly flags
        """
        df = df.copy()
        
        # Decompose time series
        series = df.set_index('date')[value_col]
        decomposition = self.decompose(series)
        
        # Calculate z-scores of residuals
        residuals = decomposition['residual'].dropna()
        
        if len(residuals) > 0:
            residual_mean = residuals.mean()
            residual_std = residuals.std()
            
            if residual_std > 0:
                z_scores = (residuals - residual_mean) / residual_std
                
                # Identify anomalies
                anomaly_mask = np.abs(z_scores) > threshold
                
                # Add to dataframe
                df['residual'] = decomposition['residual'].values
                df['residual_zscore'] = z_scores.values
                df[f'{value_col}_seasonal_anomaly'] = anomaly_mask.values
            else:
                df[f'{value_col}_seasonal_anomaly'] = False
        
        return df