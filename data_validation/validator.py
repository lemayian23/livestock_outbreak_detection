"""
Data validator with advanced validation capabilities
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Callable
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass, field
import json

from .schema import (
    DatasetSchema, ColumnSchema, DataType, ValidationSeverity,
    get_schema_registry
)

logger = logging.getLogger(__name__)


@dataclass
class ValidationRule:
    """Custom validation rule"""
    name: str
    check_fn: Callable[[pd.DataFrame], Tuple[bool, str]]
    description: str = ""
    severity: ValidationSeverity = ValidationSeverity.ERROR
    columns: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataValidator:
    """
    Advanced data validator with schema validation and custom rules
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.schema_registry = get_schema_registry()
        self.custom_rules: Dict[str, ValidationRule] = {}
        self._initialize_default_rules()
        
        logger.info("Data validator initialized")
    
    def _initialize_default_rules(self) -> None:
        """Initialize default validation rules"""
        
        # Rule: Sick animals cannot exceed total animals
        def sick_vs_total_rule(df: pd.DataFrame) -> Tuple[bool, str]:
            if 'sick_animals' in df.columns and 'total_animals' in df.columns:
                invalid_rows = df[df['sick_animals'] > df['total_animals']]
                if len(invalid_rows) > 0:
                    return False, f"Sick animals exceed total animals in {len(invalid_rows)} rows"
            return True, "Sick animals validation passed"
        
        self.register_rule(ValidationRule(
            name="sick_animals_not_exceed_total",
            check_fn=sick_vs_total_rule,
            description="Sick animals count should not exceed total animals",
            severity=ValidationSeverity.ERROR,
            columns=["sick_animals", "total_animals"]
        ))
        
        # Rule: Deceased animals cannot exceed sick animals
        def deceased_vs_sick_rule(df: pd.DataFrame) -> Tuple[bool, str]:
            if 'deceased_animals' in df.columns and 'sick_animals' in df.columns:
                invalid_rows = df[df['deceased_animals'] > df['sick_animals']]
                if len(invalid_rows) > 0:
                    return False, f"Deceased animals exceed sick animals in {len(invalid_rows)} rows"
            return True, "Deceased animals validation passed"
        
        self.register_rule(ValidationRule(
            name="deceased_not_exceed_sick",
            check_fn=deceased_vs_sick_rule,
            description="Deceased animals should not exceed sick animals",
            severity=ValidationSeverity.ERROR,
            columns=["deceased_animals", "sick_animals"]
        ))
        
        # Rule: Activity level should correlate with sickness
        def activity_sickness_rule(df: pd.DataFrame) -> Tuple[bool, str]:
            if 'activity_level' in df.columns and 'sick_animals' in df.columns and 'total_animals' in df.columns:
                # Calculate sickness percentage
                df = df.copy()
                df['sickness_pct'] = df['sick_animals'] / df['total_animals'] * 100
                
                # Find rows where high sickness but normal activity (possible data issue)
                high_sickness = df['sickness_pct'] > 20
                normal_activity = df['activity_level'] > 7
                suspicious = df[high_sickness & normal_activity]
                
                if len(suspicious) > 0:
                    return False, f"High sickness with normal activity in {len(suspicious)} rows"
            return True, "Activity-sickness correlation passed"
        
        self.register_rule(ValidationRule(
            name="activity_sickness_correlation",
            check_fn=activity_sickness_rule,
            description="High sickness should correlate with low activity",
            severity=ValidationSeverity.WARNING,
            columns=["activity_level", "sick_animals", "total_animals"]
        ))
        
        # Rule: Check for duplicate primary keys
        def duplicate_primary_key_rule(df: pd.DataFrame, schema: DatasetSchema) -> Callable[[pd.DataFrame], Tuple[bool, str]]:
            def check(df_inner: pd.DataFrame) -> Tuple[bool, str]:
                if schema.primary_key:
                    duplicates = df_inner.duplicated(subset=schema.primary_key, keep=False)
                    if duplicates.any():
                        duplicate_count = duplicates.sum()
                        return False, f"Found {duplicate_count} duplicate rows based on primary key"
                return True, "No duplicate primary keys found"
            return check
        
        # This rule will be applied per-schema
        self.duplicate_rule_generator = duplicate_primary_key_rule
        
        logger.info(f"Initialized {len(self.custom_rules)} default validation rules")
    
    def register_rule(self, rule: ValidationRule) -> None:
        """Register a custom validation rule"""
        self.custom_rules[rule.name] = rule
        logger.info(f"Registered validation rule: {rule.name}")
    
    def validate_with_schema(self, schema_name: str, data: pd.DataFrame, 
                           apply_custom_rules: bool = True) -> Dict[str, Any]:
        """
        Validate data against a schema with optional custom rules
        
        Returns:
            Comprehensive validation report
        """
        # Get schema
        schema = self.schema_registry.get_schema(schema_name)
        if not schema:
            raise ValueError(f"Schema '{schema_name}' not found")
        
        logger.info(f"Validating data against schema '{schema_name}'")
        
        # Run schema validation
        schema_report = schema.validate_dataframe(data)
        
        # Initialize validation report
        validation_report = {
            'schema_name': schema_name,
            'schema_version': schema.version,
            'timestamp': datetime.now().isoformat(),
            'schema_validation': schema_report,
            'custom_rules': {},
            'summary': {
                'total_rows': len(data),
                'schema_errors': schema_report['summary']['errors'],
                'schema_warnings': schema_report['summary']['warnings'],
                'custom_rule_errors': 0,
                'custom_rule_warnings': 0,
                'overall_is_valid': schema_report['is_valid']
            }
        }
        
        # Apply custom rules if requested
        if apply_custom_rules:
            rule_results = {}
            
            # Apply general rules
            for rule_name, rule in self.custom_rules.items():
                # Check if rule applies to this data (has required columns)
                if rule.columns:
                    missing_columns = [col for col in rule.columns if col not in data.columns]
                    if missing_columns:
                        rule_results[rule_name] = {
                            'passed': True,
                            'message': f"Skipped - missing columns: {missing_columns}",
                            'severity': 'info'
                        }
                        continue
                
                # Apply the rule
                try:
                    passed, message = rule.check_fn(data)
                    rule_results[rule_name] = {
                        'passed': passed,
                        'message': message,
                        'severity': rule.severity.value
                    }
                    
                    if not passed:
                        if rule.severity == ValidationSeverity.ERROR:
                            validation_report['summary']['custom_rule_errors'] += 1
                            validation_report['summary']['overall_is_valid'] = False
                        elif rule.severity == ValidationSeverity.WARNING:
                            validation_report['summary']['custom_rule_warnings'] += 1
                
                except Exception as e:
                    rule_results[rule_name] = {
                        'passed': False,
                        'message': f"Rule execution failed: {str(e)}",
                        'severity': 'error'
                    }
                    validation_report['summary']['custom_rule_errors'] += 1
                    validation_report['summary']['overall_is_valid'] = False
            
            # Apply schema-specific duplicate rule
            if schema.primary_key:
                duplicate_check = self.duplicate_rule_generator(data, schema)
                passed, message = duplicate_check(data)
                rule_results['duplicate_primary_key'] = {
                    'passed': passed,
                    'message': message,
                    'severity': 'error'
                }
                if not passed:
                    validation_report['summary']['custom_rule_errors'] += 1
                    validation_report['summary']['overall_is_valid'] = False
            
            validation_report['custom_rules'] = rule_results
        
        # Add overall status
        validation_report['is_valid'] = validation_report['summary']['overall_is_valid']
        
        logger.info(f"Validation complete: {'VALID' if validation_report['is_valid'] else 'INVALID'}")
        
        return validation_report
    
    def validate_batch(self, data_batch: List[Dict], schema_name: str) -> Dict[str, Any]:
        """
        Validate a batch of data records
        
        Args:
            data_batch: List of dictionaries (records)
            schema_name: Schema to validate against
            
        Returns:
            Batch validation report
        """
        if not data_batch:
            return {
                'is_valid': True,
                'message': 'Empty batch',
                'valid_count': 0,
                'invalid_count': 0
            }
        
        df = pd.DataFrame(data_batch)
        report = self.validate_with_schema(schema_name, df)
        
        # Count valid/invalid rows
        valid_rows = 0
        invalid_rows = 0
        
        if 'row_validation' in report['schema_validation']:
            for row_validation in report['schema_validation']['row_validation']:
                if row_validation['errors']:
                    invalid_rows += 1
                else:
                    valid_rows += 1
        
        # Add batch summary
        report['batch_summary'] = {
            'total_records': len(data_batch),
            'valid_records': valid_rows,
            'invalid_records': invalid_rows,
            'validation_rate': valid_rows / len(data_batch) if data_batch else 0
        }
        
        return report
    
    def create_data_quality_report(self, data: pd.DataFrame, schema_name: str) -> Dict[str, Any]:
        """
        Create comprehensive data quality report
        
        Returns:
            Data quality report with statistics and issues
        """
        # Run validation
        validation_report = self.validate_with_schema(schema_name, data)
        
        # Calculate data quality metrics
        quality_metrics = {
            'completeness': self._calculate_completeness(data),
            'accuracy': self._calculate_accuracy(data, validation_report),
            'consistency': self._calculate_consistency(data),
            'timeliness': self._calculate_timeliness(data),
            'validity': validation_report['summary']['overall_is_valid']
        }
        
        # Overall quality score (weighted average)
        weights = {
            'completeness': 0.3,
            'accuracy': 0.3,
            'consistency': 0.2,
            'timeliness': 0.1,
            'validity': 0.1
        }
        
        quality_score = sum(
            quality_metrics[metric] * weights[metric] 
            for metric in quality_metrics
            if isinstance(quality_metrics[metric], (int, float))
        )
        
        # Create quality report
        quality_report = {
            'timestamp': datetime.now().isoformat(),
            'data_shape': {
                'rows': len(data),
                'columns': len(data.columns)
            },
            'quality_metrics': quality_metrics,
            'quality_score': round(quality_score, 3),
            'quality_grade': self._get_quality_grade(quality_score),
            'validation_summary': validation_report['summary'],
            'major_issues': self._extract_major_issues(validation_report),
            'recommendations': self._generate_recommendations(validation_report, quality_metrics)
        }
        
        return quality_report
    
    def _calculate_completeness(self, data: pd.DataFrame) -> float:
        """Calculate data completeness score (0-1)"""
        if len(data) == 0:
            return 0.0
        
        total_cells = data.size
        non_null_cells = data.count().sum()
        
        return non_null_cells / total_cells if total_cells > 0 else 0.0
    
    def _calculate_accuracy(self, data: pd.DataFrame, validation_report: Dict) -> float:
        """Calculate data accuracy score (0-1)"""
        total_rows = len(data)
        if total_rows == 0:
            return 0.0
        
        # Count rows with validation errors
        error_rows = 0
        if 'row_validation' in validation_report['schema_validation']:
            for row_validation in validation_report['schema_validation']['row_validation']:
                if row_validation['errors']:
                    error_rows += 1
        
        accurate_rows = total_rows - error_rows
        return accurate_rows / total_rows if total_rows > 0 else 0.0
    
    def _calculate_consistency(self, data: pd.DataFrame) -> float:
        """Calculate data consistency score (0-1)"""
        # Simple consistency check: no contradictory values
        # This can be expanded with domain-specific rules
        
        consistency_score = 1.0
        
        # Check for logical inconsistencies
        if 'sick_animals' in data.columns and 'total_animals' in data.columns:
            inconsistent = (data['sick_animals'] > data['total_animals']).sum()
            if inconsistent > 0:
                consistency_score -= 0.2
        
        if 'deceased_animals' in data.columns and 'sick_animals' in data.columns:
            inconsistent = (data['deceased_animals'] > data['sick_animals']).sum()
            if inconsistent > 0:
                consistency_score -= 0.2
        
        return max(0.0, consistency_score)
    
    def _calculate_timeliness(self, data: pd.DataFrame) -> float:
        """Calculate data timeliness score (0-1)"""
        # Check if data is recent (within last 7 days)
        if 'date' in data.columns or 'timestamp' in data.columns:
            date_col = 'date' if 'date' in data.columns else 'timestamp'
            
            try:
                # Convert to datetime if needed
                if not pd.api.types.is_datetime64_any_dtype(data[date_col]):
                    data[date_col] = pd.to_datetime(data[date_col])
                
                # Calculate days since most recent data point
                most_recent = data[date_col].max()
                days_old = (datetime.now() - most_recent).days
                
                # Score based on freshness (0-1)
                if days_old <= 1:
                    return 1.0
                elif days_old <= 3:
                    return 0.8
                elif days_old <= 7:
                    return 0.5
                else:
                    return 0.2
            except:
                return 0.5  # Default score if date parsing fails
        
        return 0.5  # Default score if no date column
    
    def _get_quality_grade(self, score: float) -> str:
        """Convert quality score to letter grade"""
        if score >= 0.9:
            return "A"
        elif score >= 0.8:
            return "B"
        elif score >= 0.7:
            return "C"
        elif score >= 0.6:
            return "D"
        else:
            return "F"
    
    def _extract_major_issues(self, validation_report: Dict) -> List[Dict]:
        """Extract major issues from validation report"""
        issues = []
        
        # Schema validation errors
        if validation_report['schema_validation']['summary']['errors'] > 0:
            issues.append({
                'type': 'schema_error',
                'count': validation_report['schema_validation']['summary']['errors'],
                'description': 'Data does not conform to expected schema'
            })
        
        # Custom rule errors
        if validation_report['summary']['custom_rule_errors'] > 0:
            issues.append({
                'type': 'business_rule_error',
                'count': validation_report['summary']['custom_rule_errors'],
                'description': 'Data violates business rules'
            })
        
        # Column-specific issues
        for col_name, col_stats in validation_report['schema_validation']['column_stats'].items():
            if col_stats.get('error_count', 0) > 0:
                issues.append({
                    'type': 'column_error',
                    'column': col_name,
                    'count': col_stats['error_count'],
                    'description': f"Column '{col_name}' has validation errors"
                })
        
        return issues
    
    def _generate_recommendations(self, validation_report: Dict, quality_metrics: Dict) -> List[str]:
        """Generate recommendations for data quality improvement"""
        recommendations = []
        
        # Check completeness
        if quality_metrics['completeness'] < 0.9:
            recommendations.append("Increase data completeness by reducing null values")
        
        # Check accuracy
        if quality_metrics['accuracy'] < 0.9:
            recommendations.append("Improve data accuracy by fixing validation errors")
        
        # Check for specific issues
        if validation_report['schema_validation']['summary']['errors'] > 0:
            recommendations.append("Fix schema validation errors before processing")
        
        if validation_report['summary']['custom_rule_errors'] > 0:
            recommendations.append("Review and fix business rule violations")
        
        return recommendations
    
    def save_validation_report(self, report: Dict, filepath: str) -> None:
        """Save validation report to file"""
        try:
            with open(filepath, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Validation report saved to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save validation report: {str(e)}")


# Global validator instance
_data_validator: Optional[DataValidator] = None


def get_data_validator(config: Optional[Dict] = None) -> DataValidator:
    """Get or create the global data validator"""
    global _data_validator
    
    if _data_validator is None:
        _data_validator = DataValidator(config)
    
    return _data_validator


def reset_data_validator() -> None:
    """Reset the global data validator (for testing)"""
    global _data_validator
    _data_validator = None