"""
Data cleaner with missing value handling, duplicate removal, and range validation
"""
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class DataCleaner:
    """Cleans and prepares livestock data"""

    # Default biologically plausible ranges for livestock metrics.
    # Values outside these bounds get flagged as 'extreme'.
    DEFAULT_RANGES = {
        'temperature': (35.0, 41.0),
        'heart_rate': (40.0, 140.0),
        'activity_level': (0.1, 5.0),
        'feed_intake_percent': (0.0, 100.0),
        'water_intake_percent': (0.0, 100.0),
    }

    def __init__(self, config: Optional[Any] = None):
        self.config = config
        self.ranges = self._resolve_ranges(config)

    def _resolve_ranges(self, config: Any) -> Dict[str, tuple]:
        """Extract metric ranges from a dict, Config object, or None."""
        merged = dict(self.DEFAULT_RANGES)

        if config is None:
            return merged

        # Plain dict
        if isinstance(config, dict):
            ranges = config.get('metric_ranges')
            if isinstance(ranges, dict):
                merged.update(ranges)
            return merged

        # Config object with .raw_config
        raw = getattr(config, 'raw_config', None)
        if isinstance(raw, dict):
            ranges = raw.get('metric_ranges')
            if isinstance(ranges, dict):
                merged.update(ranges)
                return merged

            livestock = raw.get('livestock_metrics', {})
            if isinstance(livestock, dict):
                nested = livestock.get('normal_ranges')
                if isinstance(nested, dict):
                    for metric, values in nested.items():
                        if isinstance(values, dict):
                            lows, highs = [], []
                            for v in values.values():
                                if isinstance(v, (list, tuple)) and len(v) == 2:
                                    lows.append(v[0])
                                    highs.append(v[1])
                            if lows and highs:
                                merged[metric] = (min(lows), max(highs))
                        elif isinstance(values, (list, tuple)) and len(values) == 2:
                            merged[metric] = tuple(values)

        return merged

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Full cleaning pipeline"""
        df = df.copy()
        df = self.handle_missing_values(df)
        df = self.remove_duplicates(df)
        df = self.handle_outliers(df)
        return df

    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fill or drop missing values"""
        df = df.copy()

        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        if 'tag_id' in df.columns:
            for col in numeric_cols:
                df[col] = df.groupby('tag_id')[col].transform(
                    lambda x: x.ffill().bfill()
                )
        else:
            for col in numeric_cols:
                df[col] = df[col].ffill().bfill()

        # Fill remaining with column median
        for col in numeric_cols:
            if df[col].isna().any():
                median_val = df[col].median()
                if pd.isna(median_val):
                    median_val = 0
                df[col] = df[col].fillna(median_val)

        # Drop rows missing critical identifiers
        critical_cols = [c for c in ['date', 'farm_id', 'animal_type'] if c in df.columns]
        if critical_cols:
            df = df.dropna(subset=critical_cols)

        return df

    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate records"""
        subset = None
        if 'tag_id' in df.columns and 'date' in df.columns:
            subset = ['tag_id', 'date']
        return df.drop_duplicates(subset=subset, keep='first').reset_index(drop=True)

    def handle_outliers(self, df: pd.DataFrame, method: str = 'clip') -> pd.DataFrame:
        """Cap or clip extreme values"""
        df = df.copy()
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        for col in numeric_cols:
            # Clip activity level to >= 0.1 if present
            if col == 'activity_level':
                df[col] = df[col].clip(lower=0.1)

            if method == 'iqr':
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                if iqr == 0:
                    continue
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                df[col] = df[col].clip(lower=lower, upper=upper)

        return df

    def validate_ranges(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flag values outside biologically plausible ranges.

        For each metric with a defined range, adds '<metric>_flag' column with values:
          - 'normal'   : inside [low, high]
          - 'extreme'  : outside [low, high]
        """
        df = df.copy()

        for metric, bounds in self.ranges.items():
            if metric not in df.columns:
                continue
            if not (isinstance(bounds, (list, tuple)) and len(bounds) == 2):
                continue

            low, high = bounds
            flag_col = f'{metric}_flag'

            # Default 'normal', then mark 'extreme' where out of range
            df[flag_col] = 'normal'
            out_of_range = (df[metric] < low) | (df[metric] > high)
            df.loc[out_of_range, flag_col] = 'extreme'

        return df

    # Backwards-compatible aliases
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.clean_dataframe(df)

    def handle_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.handle_missing_values(df)