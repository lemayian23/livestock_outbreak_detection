"""
Tests for data validation system
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tempfile
import json
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_validation.schema import (
    DatasetSchema, ColumnSchema, DataType, ValidationSeverity,
    SchemaRegistry, get_schema_registry, reset_schema_registry
)
from data_validation.validator import (
    DataValidator, ValidationRule, get_data_validator, reset_data_validator
)


class TestColumnSchema:
    def test_column_validation_integer(self):
        """Test integer column validation"""
        col = ColumnSchema(
            name="age",
            data_type=DataType.INTEGER,
            min_value=0,
            max_value=100
        )
        
        # Valid cases
        assert col.validate(25)[0] == True
        assert col.validate("25")[0] == True  # String conversion
        assert col.validate(25.0)[0] == True  # Float conversion
        
        # Invalid cases
        assert col.validate(-5)[0] == False  # Below min
        assert col.validate(150)[0] == False  # Above max
        assert col.validate("abc")[0] == False  # Not a number
    
    def test_column_validation_string_pattern(self):
        """Test string column validation with pattern"""
        col = ColumnSchema(
            name="id",
            data_type=DataType.STRING,
            pattern=r'^FARM-\d{4}$'
        )
        
        assert col.validate("FARM-1234")[0] == True
        assert col.validate("farm-1234")[0] == False  # Lowercase
        assert col.validate("FARM-123")[0] == False  # Wrong format
        assert col.validate("TEST-1234")[0] == False  # Wrong prefix
    
    def test_column_validation_categorical(self):
        """Test categorical column validation"""
        col = ColumnSchema(
            name="animal_type",
            data_type=DataType.CATEGORICAL,
            allowed_values=["cattle", "swine", "poultry"]
        )
        
        assert col.validate("cattle")[0] == True
        assert col.validate("swine")[0] == True
        assert col.validate("poultry")[0] == True
        assert col.validate("sheep")[0] == False  # Not in allowed values
        assert col.validate("CATTLE")[0] == False  # Case sensitive
    
    def test_column_validation_nullable(self):
        """Test nullable column validation"""
        col_nullable = ColumnSchema(
            name="optional",
            data_type=DataType.STRING,
            nullable=True
        )
        
        col_required = ColumnSchema(
            name="required",
            data_type=DataType.STRING,
            nullable=False
        )
        
        assert col_nullable.validate(None)[0] == True
        assert col_nullable.validate(np.nan)[0] == True
        assert col_required.validate(None)[0] == False
        assert col_required.validate(np.nan)[0] == False


class TestDatasetSchema:
    def setup_method(self):
        """Create a test schema"""
        self.schema = DatasetSchema(
            name="test_schema",
            version="1.0",
            columns=[
                ColumnSchema(
                    name="id",
                    data_type=DataType.INTEGER,
                    min_value=1
                ),
                ColumnSchema(
                    name="name",
                    data_type=DataType.STRING,
                    required=True
                ),
                ColumnSchema(
                    name="score",
                    data_type=DataType.FLOAT,
                    min_value=0,
                    max_value=100
                )
            ],
            required_columns=["id", "name"]
        )
    
    def test_row_validation(self):
        """Test single row validation"""
        # Valid row
        valid_row = {"id": 1, "name": "Test", "score": 85.5}
        results = self.schema.validate_row(valid_row)
        assert len(results['errors']) == 0
        
        # Invalid row (missing required column)
        invalid_row = {"id": 1, "score": 85.5}
        results = self.schema.validate_row(invalid_row)
        assert len(results['errors']) > 0
        
        # Invalid row (score out of range)
        invalid_row2 = {"id": 1, "name": "Test", "score": 150}
        results = self.schema.validate_row(invalid_row2)
        assert len(results['errors']) > 0
    
    def test_dataframe_validation(self):
        """Test DataFrame validation"""
        # Create test DataFrame
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "score": [85.5, 92.0, 78.5]
        })
        
        report = self.schema.validate_dataframe(df)
        
        assert report['is_valid'] == True
        assert report['summary']['total_rows'] == 3
        assert report['summary']['errors'] == 0
    
    def test_to_from_dict(self):
        """Test schema serialization/deserialization"""
        schema_dict = self.schema.to_dict()
        
        # Convert back to schema
        new_schema = DatasetSchema.from_dict(schema_dict)
        
        assert new_schema.name == self.schema.name
        assert new_schema.version == self.schema.version
        assert len(new_schema.columns) == len(self.schema.columns)
        assert new_schema.columns[0].name == self.schema.columns[0].name
    
    def test_to_from_json(self):
        """Test schema JSON serialization"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name
        
        try:
            # Save to JSON
            self.schema.to_json(temp_path)
            
            # Load from JSON
            loaded_schema = DatasetSchema.from_json(temp_path)
            
            assert loaded_schema.name == self.schema.name
            assert len(loaded_schema.columns) == len(self.schema.columns)
            
        finally:
            os.unlink(temp_path)


class TestSchemaRegistry:
    def setup_method(self):
        reset_schema_registry()
    
    def test_default_schemas(self):
        """Test that default schemas are loaded"""
        registry = get_schema_registry()
        
        assert len(registry.list_schemas()) > 0
        assert 'daily_health_metrics' in registry.list_schemas()
        assert 'outbreak_alerts' in registry.list_schemas()
        assert 'environmental_data' in registry.list_schemas()
    
    def test_get_schema(self):
        """Test getting schema by name"""
        registry = get_schema_registry()
        
        schema = registry.get_schema('daily_health_metrics')
        assert schema is not None
        assert schema.name == 'daily_health_metrics'
        assert len(schema.columns) > 0
    
    def test_validate_data(self):
        """Test data validation through registry"""
        registry = get_schema_registry()
        
        # Create valid test data
        test_data = pd.DataFrame({
            "farm_id": ["FARM-0001"],
            "date": ["2024-01-01"],
            "animal_type": ["cattle"],
            "total_animals": [100],
            "sick_animals": [5],
            "deceased_animals": [1],
            "avg_temperature": [38.5],
            "feed_intake_percent": [85.0],
            "water_intake_percent": [90.0],
            "activity_level": [7.5],
            "location_lat": [40.7128],
            "location_lon": [-74.0060]
        })
        
        report = registry.validate_data('daily_health_metrics', test_data)
        
        assert report['is_valid'] == True
    
    def test_validate_invalid_data(self):
        """Test validation of invalid data"""
        registry = get_schema_registry()
        
        # Create invalid test data (missing required column)
        test_data = pd.DataFrame({
            "farm_id": ["FARM-0001"],
            "date": ["2024-01-01"],
            # Missing animal_type and total_animals
            "sick_animals": [5]
        })
        
        report = registry.validate_data('daily_health_metrics', test_data)
        
        assert report['is_valid'] == False
        assert report['summary']['errors'] > 0


class TestDataValidator:
    def setup_method(self):
        reset_data_validator()
    
    def test_initialization(self):
        """Test validator initialization"""
        validator = DataValidator()
        
        assert validator.schema_registry is not None
        assert len(validator.custom_rules) > 0
    
    def test_validate_with_schema(self):
        """Test schema-based validation"""
        validator = DataValidator()
        
        # Create test data
        test_data = pd.DataFrame({
            "farm_id": ["FARM-0001", "FARM-0002"],
            "date": ["2024-01-01", "2024-01-02"],
            "animal_type": ["cattle", "swine"],
            "total_animals": [100, 200],
            "sick_animals": [5, 10],
            "deceased_animals": [1, 2],
            "avg_temperature": [38.5, 39.0],
            "feed_intake_percent": [85.0, 90.0],
            "water_intake_percent": [90.0, 85.0],
            "activity_level": [7.5, 8.0],
            "location_lat": [40.7128, 41.8781],
            "location_lon": [-74.0060, -87.6298]
        })
        
        report = validator.validate_with_schema('daily_health_metrics', test_data)
        
        assert 'schema_validation' in report
        assert 'custom_rules' in report
        assert 'summary' in report
        assert report['schema_name'] == 'daily_health_metrics'
    
    def test_custom_rule_violation(self):
        """Test custom rule validation"""
        validator = DataValidator()
        
        # Create test data with rule violation
        # sick_animals > total_animals in first row
        test_data = pd.DataFrame({
            "farm_id": ["FARM-0001", "FARM-0002"],
            "date": ["2024-01-01", "2024-01-02"],
            "animal_type": ["cattle", "swine"],
            "total_animals": [100, 200],
            "sick_animals": [150, 10],  # Violation: 150 > 100
            "deceased_animals": [1, 2],
            "avg_temperature": [38.5, 39.0],
            "feed_intake_percent": [85.0, 90.0],
            "water_intake_percent": [90.0, 85.0],
            "activity_level": [7.5, 8.0],
            "location_lat": [40.7128, 41.8781],
            "location_lon": [-74.0060, -87.6298]
        })
        
        report = validator.validate_with_schema('daily_health_metrics', test_data)
        
        # Check that custom rule caught the violation
        assert 'sick_animals_not_exceed_total' in report['custom_rules']
        rule_result = report['custom_rules']['sick_animals_not_exceed_total']
        assert rule_result['passed'] == False
        assert 'exceed' in rule_result['message'].lower()
    
    def test_create_data_quality_report(self):
        """Test data quality report generation"""
        validator = DataValidator()
        
        # Create test data
        test_data = pd.DataFrame({
            "farm_id": ["FARM-0001", "FARM-0002", "FARM-0003"],
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "animal_type": ["cattle", "swine", "poultry"],
            "total_animals": [100, 200, 150],
            "sick_animals": [5, 10, 8],
            "deceased_animals": [1, 2, 1],
            "avg_temperature": [38.5, 39.0, 40.0],
            "feed_intake_percent": [85.0, 90.0, 80.0],
            "water_intake_percent": [90.0, 85.0, 88.0],
            "activity_level": [7.5, 8.0, 7.0],
            "location_lat": [40.7128, 41.8781, 34.0522],
            "location_lon": [-74.0060, -87.6298, -118.2437]
        })
        
        quality_report = validator.create_data_quality_report(
            test_data, 'daily_health_metrics'
        )
        
        assert 'quality_metrics' in quality_report
        assert 'quality_score' in quality_report
        assert 'quality_grade' in quality_report
        assert 'data_shape' in quality_report
        
        # Check metrics
        metrics = quality_report['quality_metrics']
        assert 'completeness' in metrics
        assert 'accuracy' in metrics
        assert 'consistency' in metrics
        assert 'timeliness' in metrics
        assert 'validity' in metrics
        
        # Score should be between 0 and 1
        assert 0 <= quality_report['quality_score'] <= 1
        
        # Grade should be one of A-F
        assert quality_report['quality_grade'] in ['A', 'B', 'C', 'D', 'F']
    
    def test_register_custom_rule(self):
        """Test registering custom validation rule"""
        validator = DataValidator()
        
        # Create a custom rule
        def custom_check(df):
            # Check if all farm IDs start with 'FARM-'
            invalid = df[~df['farm_id'].str.startswith('FARM-')]
            if len(invalid) > 0:
                return False, f"Found {len(invalid)} invalid farm IDs"
            return True, "All farm IDs valid"
        
        rule = ValidationRule(
            name="farm_id_format",
            check_fn=custom_check,
            description="Farm IDs must start with 'FARM-'",
            severity=ValidationSeverity.ERROR,
            columns=["farm_id"]
        )
        
        validator.register_rule(rule)
        
        assert 'farm_id_format' in validator.custom_rules
    
    def test_save_validation_report(self):
        """Test saving validation report to file"""
        validator = DataValidator()
        
        # Create test data and report
        test_data = pd.DataFrame({
            "farm_id": ["FARM-0001"],
            "date": ["2024-01-01"],
            "animal_type": ["cattle"],
            "total_animals": [100],
            "sick_animals": [5],
            "deceased_animals": [1],
            "avg_temperature": [38.5],
            "feed_intake_percent": [85.0],
            "water_intake_percent": [90.0],
            "activity_level": [7.5],
            "location_lat": [40.7128],
            "location_lon": [-74.0060]
        })
        
        report = validator.validate_with_schema('daily_health_metrics', test_data)
        
        # Save to temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name
        
        try:
            validator.save_validation_report(report, temp_path)
            
            # Verify file was created and contains valid JSON
            with open(temp_path, 'r') as f:
                loaded_report = json.load(f)
            
            assert loaded_report['schema_name'] == 'daily_health_metrics'
            assert 'is_valid' in loaded_report
            
        finally:
            os.unlink(temp_path)


class TestGlobalInstances:
    def test_singleton_patterns(self):
        """Test that global instances follow singleton pattern"""
        # Schema registry
        registry1 = get_schema_registry()
        registry2 = get_schema_registry()
        assert registry1 is registry2
        
        # Data validator
        validator1 = get_data_validator()
        validator2 = get_data_validator()
        assert validator1 is validator2
    
    def test_reset_functions(self):
        """Test reset functions for global instances"""
        # Get instances
        registry1 = get_schema_registry()
        validator1 = get_data_validator()
        
        # Reset
        reset_schema_registry()
        reset_data_validator()
        
        # Get new instances
        registry2 = get_schema_registry()
        validator2 = get_data_validator()
        
        # Should be different instances
        assert registry1 is not registry2
        assert validator1 is not validator2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])