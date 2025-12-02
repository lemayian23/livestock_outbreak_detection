"""
Tests for data handling and preprocessing
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime

from src.preprocessing.cleaner import DataCleaner
from src.utils.config import Config

class TestDataHandling(unittest.TestCase):
    
    def setUp(self):
        # Create test config
        self.config = Config()
        
        # Create test data
        dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
        self.test_data = pd.DataFrame({
            'tag_id': ['ANM001'] * 10,
            'date': dates,
            'animal_type': ['cattle'] * 10,
            'temperature': [38.5, 38.7, 39.0, 40.5, 39.2, np.nan, 38.8, 39.1, 39.3, 38.9],
            'heart_rate': [70, 72, 75, 85, 78, 80, 73, 76, 79, 74],
            'activity_level': [1.0, 1.2, 0.9, 0.5, 1.1, 1.0, 1.3, 0.8, 1.0, 1.1]
        })
    
    def test_data_cleaning(self):
        """Test data cleaning functionality"""
        cleaner = DataCleaner(self.config)
        cleaned = cleaner.clean_dataframe(self.test_data)
        
        # Check that NaN is filled
        self.assertFalse(cleaned['temperature'].isnull().any())
        
        # Check data types
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(cleaned['date']))
        
        # Check activity level clipping
        self.assertTrue((cleaned['activity_level'] >= 0.1).all())
    
    def test_missing_value_handling(self):
        """Test missing value imputation"""
        cleaner = DataCleaner(self.config)
        
        # Create data with missing values
        data_with_nan = self.test_data.copy()
        data_with_nan.loc[5, 'temperature'] = np.nan
        data_with_nan.loc[3, 'heart_rate'] = np.nan
        
        cleaned = cleaner.handle_missing_values(data_with_nan)
        
        # Check that NaNs are filled
        self.assertFalse(cleaned['temperature'].isnull().any())
        self.assertFalse(cleaned['heart_rate'].isnull().any())
    
    def test_duplicate_removal(self):
        """Test duplicate record removal"""
        cleaner = DataCleaner(self.config)
        
        # Create data with duplicates
        data_with_dups = pd.concat([self.test_data, self.test_data.iloc[[0, 1]]])
        
        cleaned = cleaner.remove_duplicates(data_with_dups)
        
        # Should have original number of rows
        self.assertEqual(len(cleaned), len(self.test_data))
    
    def test_range_validation(self):
        """Test range validation for health metrics"""
        cleaner = DataCleaner(self.config)
        
        # Create data with extreme value
        extreme_data = self.test_data.copy()
        extreme_data.loc[3, 'temperature'] = 42.0  # Very high temperature
        
        validated = cleaner.validate_ranges(extreme_data)
        
        # Should have flag for extreme temperature
        self.assertIn('temperature_flag', validated.columns)
        self.assertEqual(validated.loc[3, 'temperature_flag'], 'extreme')

if __name__ == '__main__':
    unittest.main()