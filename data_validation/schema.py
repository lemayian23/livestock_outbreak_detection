"""
Data schema definitions and validation for livestock outbreak detection
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime
import json
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class DataType(Enum):
    """Supported data types for validation"""
    INTEGER = "integer"
    FLOAT = "float"
    STRING = "string"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    CATEGORICAL = "categorical"
    PERCENTAGE = "percentage"
    COUNT = "count"
    TEMPERATURE = "temperature"
    WEIGHT = "weight"


class ValidationSeverity(Enum):
    """Validation severity levels"""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ColumnSchema:
    """Schema definition for a single column"""
    name: str
    data_type: DataType
    required: bool = True
    nullable: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allowed_values: Optional[List[Any]] = None
    pattern: Optional[str] = None  # Regex pattern for strings
    description: str = ""
    default_value: Optional[Any] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def validate(self, value: Any) -> Tuple[bool, str, ValidationSeverity]:
        """
        Validate a single value against this schema
        
        Returns:
            Tuple of (is_valid, message, severity)
        """
        # Handle null values
        if pd.isna(value) or value is None:
            if not self.nullable:
                return False, f"Column '{self.name}' cannot be null", ValidationSeverity.ERROR
            return True, "Null value allowed", ValidationSeverity.INFO
        
        # Type validation
        try:
            if self.data_type == DataType.INTEGER:
                if not isinstance(value, (int, np.integer)):
                    # Try to convert
                    int_value = int(float(value))
                    if float(value) != int_value:
                        return False, f"Column '{self.name}' must be integer, got {value}", ValidationSeverity.ERROR
                    value = int_value
                
            elif self.data_type == DataType.FLOAT:
                if not isinstance(value, (float, int, np.floating, np.integer)):
                    value = float(value)
                
            elif self.data_type == DataType.STRING:
                value = str(value)
                
            elif self.data_type == DataType.BOOLEAN:
                if isinstance(value, str):
                    value_lower = value.lower()
                    if value_lower in ['true', '1', 'yes', 'y']:
                        value = True
                    elif value_lower in ['false', '0', 'no', 'n']:
                        value = False
                    else:
                        return False, f"Column '{self.name}' must be boolean, got {value}", ValidationSeverity.ERROR
                elif not isinstance(value, (bool, np.bool_)):
                    return False, f"Column '{self.name}' must be boolean, got {value}", ValidationSeverity.ERROR
            
            elif self.data_type == DataType.DATETIME:
                if isinstance(value, str):
                    try:
                        # Try common formats
                        for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y', '%m/%d/%Y']:
                            try:
                                datetime.strptime(value, fmt)
                                break
                            except ValueError:
                                continue
                        else:
                            return False, f"Column '{self.name}' datetime format not recognized: {value}", ValidationSeverity.ERROR
                    except Exception:
                        return False, f"Column '{self.name}' must be datetime, got {value}", ValidationSeverity.ERROR
                elif not isinstance(value, (datetime, pd.Timestamp)):
                    return False, f"Column '{self.name}' must be datetime, got {value}", ValidationSeverity.ERROR
            
            elif self.data_type == DataType.PERCENTAGE:
                if not isinstance(value, (float, int, np.floating, np.integer)):
                    try:
                        value = float(value)
                    except:
                        return False, f"Column '{self.name}' must be percentage, got {value}", ValidationSeverity.ERROR
                if not (0 <= value <= 100):
                    return False, f"Column '{self.name}' must be between 0 and 100, got {value}", ValidationSeverity.ERROR
            
            elif self.data_type == DataType.TEMPERATURE:
                if not isinstance(value, (float, int, np.floating, np.integer)):
                    try:
                        value = float(value)
                    except:
                        return False, f"Column '{self.name}' must be temperature, got {value}", ValidationSeverity.ERROR
                # Reasonable temperature range for livestock (in Celsius)
                if not (-50 <= value <= 50):
                    return False, f"Column '{self.name}' temperature unrealistic: {value}°C", ValidationSeverity.WARNING
            
            elif self.data_type == DataType.WEIGHT:
                if not isinstance(value, (float, int, np.floating, np.integer)):
                    try:
                        value = float(value)
                    except:
                        return False, f"Column '{self.name}' must be weight, got {value}", ValidationSeverity.ERROR
                if value < 0:
                    return False, f"Column '{self.name}' weight cannot be negative: {value}", ValidationSeverity.ERROR
            
        except (ValueError, TypeError) as e:
            return False, f"Column '{self.name}' type conversion failed: {str(e)}", ValidationSeverity.ERROR
        
        # Range validation
        if self.min_value is not None and value < self.min_value:
            return False, f"Column '{self.name}' value {value} below minimum {self.min_value}", ValidationSeverity.ERROR
        
        if self.max_value is not None and value > self.max_value:
            return False, f"Column '{self.name}' value {value} above maximum {self.max_value}", ValidationSeverity.ERROR
        
        # Allowed values validation
        if self.allowed_values is not None and value not in self.allowed_values:
            return False, f"Column '{self.name}' value {value} not in allowed values", ValidationSeverity.ERROR
        
        # Pattern validation (for strings)
        if self.pattern is not None and isinstance(value, str):
            import re
            if not re.match(self.pattern, value):
                return False, f"Column '{self.name}' value doesn't match pattern", ValidationSeverity.ERROR
        
        return True, f"Column '{self.name}' validation passed", ValidationSeverity.INFO


@dataclass
class DatasetSchema:
    """Schema for an entire dataset"""
    name: str
    version: str = "1.0"
    description: str = ""
    columns: List[ColumnSchema] = field(default_factory=list)
    required_columns: List[str] = field(default_factory=list)
    primary_key: Optional[List[str]] = None
    row_count_range: Optional[Tuple[int, int]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_column(self, column_name: str) -> Optional[ColumnSchema]:
        """Get column schema by name"""
        for col in self.columns:
            if col.name == column_name:
                return col
        return None
    
    def validate_row(self, row: Dict[str, Any]) -> Dict[str, List[Dict]]:
        """
        Validate a single row against the schema
        
        Returns:
            Dictionary with validation results by severity
        """
        results = {
            'errors': [],
            'warnings': [],
            'infos': []
        }
        
        # Check required columns
        for col_name in self.required_columns:
            if col_name not in row or pd.isna(row.get(col_name)):
                results['errors'].append({
                    'column': col_name,
                    'message': f"Required column '{col_name}' is missing or null",
                    'value': None
                })
        
        # Validate each column that exists in the row
        for col_schema in self.columns:
            if col_schema.name in row:
                value = row[col_schema.name]
                is_valid, message, severity = col_schema.validate(value)
                
                result_item = {
                    'column': col_schema.name,
                    'message': message,
                    'value': value
                }
                
                if not is_valid:
                    if severity == ValidationSeverity.ERROR:
                        results['errors'].append(result_item)
                    elif severity == ValidationSeverity.WARNING:
                        results['warnings'].append(result_item)
                    else:
                        results['infos'].append(result_item)
                elif severity == ValidationSeverity.INFO:
                    results['infos'].append(result_item)
        
        return results
    
    def validate_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate an entire DataFrame against the schema
        
        Returns:
            Comprehensive validation report
        """
        logger.info(f"Validating DataFrame against schema '{self.name}' v{self.version}")
        
        validation_results = {
            'schema_name': self.name,
            'schema_version': self.version,
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_rows': len(df),
                'total_columns_checked': len(self.columns),
                'errors': 0,
                'warnings': 0,
                'infos': 0
            },
            'column_stats': {},
            'row_validation': [],
            'issues_by_column': {},
            'is_valid': True
        }
        
        # Initialize column stats
        for col_schema in self.columns:
            validation_results['column_stats'][col_schema.name] = {
                'data_type': col_schema.data_type.value,
                'required': col_schema.required,
                'null_count': 0,
                'error_count': 0,
                'warning_count': 0
            }
            validation_results['issues_by_column'][col_schema.name] = {
                'errors': [],
                'warnings': []
            }
        
        # Check for missing columns
        df_columns = set(df.columns)
        schema_columns = {col.name for col in self.columns}
        
        missing_columns = schema_columns - df_columns
        extra_columns = df_columns - schema_columns
        
        if missing_columns:
            for col in missing_columns:
                col_schema = self.get_column(col)
                if col_schema and col_schema.required:
                    validation_results['summary']['errors'] += 1
                    validation_results['is_valid'] = False
                    validation_results['issues_by_column'][col]['errors'].append(
                        f"Required column missing from data"
                    )
        
        if extra_columns:
            validation_results['extra_columns'] = list(extra_columns)
            logger.warning(f"Found extra columns not in schema: {extra_columns}")
        
        # Validate each row
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            row_results = self.validate_row(row_dict)
            
            # Count issues
            validation_results['summary']['errors'] += len(row_results['errors'])
            validation_results['summary']['warnings'] += len(row_results['warnings'])
            validation_results['summary']['infos'] += len(row_results['infos'])
            
            # Store row validation if there are issues
            if row_results['errors'] or row_results['warnings']:
                validation_results['row_validation'].append({
                    'row_index': idx,
                    'errors': row_results['errors'],
                    'warnings': row_results['warnings']
                })
            
            # Update column stats
            for col_schema in self.columns:
                col_name = col_schema.name
                if col_name in df.columns:
                    value = row_dict.get(col_name)
                    if pd.isna(value):
                        validation_results['column_stats'][col_name]['null_count'] += 1
        
        # Check row count range
        if self.row_count_range:
            min_rows, max_rows = self.row_count_range
            if len(df) < min_rows:
                validation_results['summary']['warnings'] += 1
                validation_results['row_count_issue'] = f"Only {len(df)} rows, expected at least {min_rows}"
            elif len(df) > max_rows:
                validation_results['summary']['warnings'] += 1
                validation_results['row_count_issue'] = f"{len(df)} rows, expected at most {max_rows}"
        
        # Determine overall validity
        if validation_results['summary']['errors'] > 0:
            validation_results['is_valid'] = False
        
        logger.info(f"Validation complete: {validation_results['summary']['errors']} errors, "
                   f"{validation_results['summary']['warnings']} warnings")
        
        return validation_results
    
    def to_dict(self) -> Dict:
        """Convert schema to dictionary"""
        return {
            'name': self.name,
            'version': self.version,
            'description': self.description,
            'columns': [
                {
                    'name': col.name,
                    'data_type': col.data_type.value,
                    'required': col.required,
                    'nullable': col.nullable,
                    'min_value': col.min_value,
                    'max_value': col.max_value,
                    'allowed_values': col.allowed_values,
                    'pattern': col.pattern,
                    'description': col.description
                }
                for col in self.columns
            ],
            'required_columns': self.required_columns,
            'primary_key': self.primary_key,
            'row_count_range': self.row_count_range,
            'metadata': self.metadata
        }
    
    def to_json(self, filepath: Optional[str] = None) -> str:
        """Convert schema to JSON"""
        schema_dict = self.to_dict()
        json_str = json.dumps(schema_dict, indent=2, default=str)
        
        if filepath:
            with open(filepath, 'w') as f:
                f.write(json_str)
            logger.info(f"Schema saved to {filepath}")
        
        return json_str
    
    @classmethod
    def from_dict(cls, schema_dict: Dict) -> 'DatasetSchema':
        """Create schema from dictionary"""
        columns = []
        for col_dict in schema_dict.get('columns', []):
            column = ColumnSchema(
                name=col_dict['name'],
                data_type=DataType(col_dict['data_type']),
                required=col_dict.get('required', True),
                nullable=col_dict.get('nullable', False),
                min_value=col_dict.get('min_value'),
                max_value=col_dict.get('max_value'),
                allowed_values=col_dict.get('allowed_values'),
                pattern=col_dict.get('pattern'),
                description=col_dict.get('description', '')
            )
            columns.append(column)
        
        return cls(
            name=schema_dict['name'],
            version=schema_dict.get('version', '1.0'),
            description=schema_dict.get('description', ''),
            columns=columns,
            required_columns=schema_dict.get('required_columns', []),
            primary_key=schema_dict.get('primary_key'),
            row_count_range=schema_dict.get('row_count_range'),
            metadata=schema_dict.get('metadata', {})
        )
    
    @classmethod
    def from_json(cls, filepath: str) -> 'DatasetSchema':
        """Load schema from JSON file"""
        with open(filepath, 'r') as f:
            schema_dict = json.load(f)
        return cls.from_dict(schema_dict)


class SchemaRegistry:
    """Registry for managing multiple schemas"""
    
    def __init__(self):
        self.schemas: Dict[str, DatasetSchema] = {}
        self._initialize_default_schemas()
    
    def _initialize_default_schemas(self) -> None:
        """Initialize default schemas for livestock outbreak detection"""
        
        # Schema for daily livestock health metrics
        daily_health_schema = DatasetSchema(
            name="daily_health_metrics",
            version="1.0",
            description="Daily health metrics for livestock",
            columns=[
                ColumnSchema(
                    name="farm_id",
                    data_type=DataType.STRING,
                    pattern=r'^FARM-\d{4}$',
                    description="Farm identifier"
                ),
                ColumnSchema(
                    name="date",
                    data_type=DataType.DATETIME,
                    description="Date of measurement"
                ),
                ColumnSchema(
                    name="animal_type",
                    data_type=DataType.CATEGORICAL,
                    allowed_values=["cattle", "swine", "poultry", "sheep", "goat"],
                    description="Type of animal"
                ),
                ColumnSchema(
                    name="total_animals",
                    data_type=DataType.INTEGER,
                    min_value=0,
                    max_value=10000,
                    description="Total number of animals"
                ),
                ColumnSchema(
                    name="sick_animals",
                    data_type=DataType.INTEGER,
                    min_value=0,
                    description="Number of sick animals"
                ),
                ColumnSchema(
                    name="deceased_animals",
                    data_type=DataType.INTEGER,
                    min_value=0,
                    description="Number of deceased animals"
                ),
                ColumnSchema(
                    name="avg_temperature",
                    data_type=DataType.TEMPERATURE,
                    min_value=35,
                    max_value=45,
                    description="Average body temperature (°C)"
                ),
                ColumnSchema(
                    name="feed_intake_percent",
                    data_type=DataType.PERCENTAGE,
                    description="Percentage of normal feed intake"
                ),
                ColumnSchema(
                    name="water_intake_percent",
                    data_type=DataType.PERCENTAGE,
                    description="Percentage of normal water intake"
                ),
                ColumnSchema(
                    name="activity_level",
                    data_type=DataType.FLOAT,
                    min_value=0,
                    max_value=10,
                    description="Activity level score (0-10)"
                ),
                ColumnSchema(
                    name="location_lat",
                    data_type=DataType.FLOAT,
                    min_value=-90,
                    max_value=90,
                    description="Latitude"
                ),
                ColumnSchema(
                    name="location_lon",
                    data_type=DataType.FLOAT,
                    min_value=-180,
                    max_value=180,
                    description="Longitude"
                )
            ],
            required_columns=["farm_id", "date", "animal_type", "total_animals"],
            primary_key=["farm_id", "date", "animal_type"],
            row_count_range=(1, 1000)
        )
        
        self.register_schema(daily_health_schema)
        
        # Schema for outbreak alerts
        alert_schema = DatasetSchema(
            name="outbreak_alerts",
            version="1.0",
            description="Outbreak alert records",
            columns=[
                ColumnSchema(
                    name="alert_id",
                    data_type=DataType.STRING,
                    pattern=r'^ALERT-\d{8}-\d{6}$',
                    description="Unique alert identifier"
                ),
                ColumnSchema(
                    name="timestamp",
                    data_type=DataType.DATETIME,
                    description="Alert timestamp"
                ),
                ColumnSchema(
                    name="farm_id",
                    data_type=DataType.STRING,
                    description="Farm identifier"
                ),
                ColumnSchema(
                    name="severity",
                    data_type=DataType.CATEGORICAL,
                    allowed_values=["low", "medium", "high", "critical"],
                    description="Alert severity"
                ),
                ColumnSchema(
                    name="anomaly_score",
                    data_type=DataType.FLOAT,
                    min_value=0,
                    max_value=1,
                    description="Anomaly detection score"
                ),
                ColumnSchema(
                    name="sick_count",
                    data_type=DataType.INTEGER,
                    min_value=0,
                    description="Number of sick animals"
                ),
                ColumnSchema(
                    name="description",
                    data_type=DataType.STRING,
                    nullable=True,
                    description="Alert description"
                ),
                ColumnSchema(
                    name="status",
                    data_type=DataType.CATEGORICAL,
                    allowed_values=["new", "investigating", "confirmed", "false_positive", "resolved"],
                    description="Alert status"
                )
            ],
            required_columns=["alert_id", "timestamp", "farm_id", "severity"],
            primary_key=["alert_id"]
        )
        
        self.register_schema(alert_schema)
        
        # Schema for environmental data
        environmental_schema = DatasetSchema(
            name="environmental_data",
            version="1.0",
            description="Environmental conditions data",
            columns=[
                ColumnSchema(
                    name="location_id",
                    data_type=DataType.STRING,
                    description="Location identifier"
                ),
                ColumnSchema(
                    name="timestamp",
                    data_type=DataType.DATETIME,
                    description="Measurement timestamp"
                ),
                ColumnSchema(
                    name="temperature",
                    data_type=DataType.TEMPERATURE,
                    min_value=-30,
                    max_value=50,
                    description="Ambient temperature (°C)"
                ),
                ColumnSchema(
                    name="humidity",
                    data_type=DataType.PERCENTAGE,
                    description="Relative humidity (%)"
                ),
                ColumnSchema(
                    name="air_quality_index",
                    data_type=DataType.FLOAT,
                    min_value=0,
                    max_value=500,
                    description="Air quality index"
                ),
                ColumnSchema(
                    name="precipitation_mm",
                    data_type=DataType.FLOAT,
                    min_value=0,
                    description="Precipitation in mm"
                ),
                ColumnSchema(
                    name="wind_speed",
                    data_type=DataType.FLOAT,
                    min_value=0,
                    max_value=200,
                    description="Wind speed (km/h)"
                )
            ],
            required_columns=["location_id", "timestamp", "temperature"],
            primary_key=["location_id", "timestamp"]
        )
        
        self.register_schema(environmental_schema)
        
        logger.info(f"Initialized schema registry with {len(self.schemas)} schemas")
    
    def register_schema(self, schema: DatasetSchema) -> None:
        """Register a new schema"""
        self.schemas[schema.name] = schema
        logger.info(f"Registered schema: {schema.name} v{schema.version}")
    
    def get_schema(self, schema_name: str) -> Optional[DatasetSchema]:
        """Get schema by name"""
        return self.schemas.get(schema_name)
    
    def list_schemas(self) -> List[str]:
        """List all registered schema names"""
        return list(self.schemas.keys())
    
    def validate_data(self, schema_name: str, data: Union[pd.DataFrame, Dict, List]) -> Dict[str, Any]:
        """
        Validate data against a schema
        
        Args:
            schema_name: Name of the schema to use
            data: Data to validate (DataFrame, dict, or list of dicts)
            
        Returns:
            Validation report
        """
        schema = self.get_schema(schema_name)
        if not schema:
            raise ValueError(f"Schema '{schema_name}' not found")
        
        # Convert to DataFrame if needed
        if isinstance(data, dict):
            data = pd.DataFrame([data])
        elif isinstance(data, list):
            data = pd.DataFrame(data)
        
        return schema.validate_dataframe(data)
    
    def is_valid(self, schema_name: str, data: Union[pd.DataFrame, Dict, List]) -> bool:
        """
        Check if data is valid according to schema
        
        Returns:
            True if data is valid, False otherwise
        """
        try:
            report = self.validate_data(schema_name, data)
            return report.get('is_valid', False)
        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            return False


# Global schema registry instance
_schema_registry: Optional[SchemaRegistry] = None


def get_schema_registry() -> SchemaRegistry:
    """Get or create the global schema registry"""
    global _schema_registry
    
    if _schema_registry is None:
        _schema_registry = SchemaRegistry()
    
    return _schema_registry


def reset_schema_registry() -> None:
    """Reset the global schema registry (for testing)"""
    global _schema_registry
    _schema_registry = None