"""
Data validator with advanced validation capabilities
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Callable, Tuple
from datetime import datetime
import logging
from dataclasses import dataclass, field
import json

from .schema import (
    DatasetSchema,
    ColumnSchema,
    DataType,
    ValidationSeverity,
    get_schema_registry,
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
    """Advanced data validator with schema validation and custom rules"""

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.schema_registry = get_schema_registry()
        self.custom_rules: Dict[str, ValidationRule] = {}
        self._initialize_default_rules()
        logger.info("Data validator initialized")

    def _initialize_default_rules(self) -> None:
        """Initialize default validation rules"""

        def sick_vs_total_rule(df: pd.DataFrame) -> Tuple[bool, str]:
            if 'sick_animals' in df.columns and 'total_animals' in df.columns:
                invalid = df[df['sick_animals'] > df['total_animals']]
                if len(invalid) > 0:
                    return False, f"Sick animals exceed total in {len(invalid)} rows"
            return True, "Sick animals validation passed"

        self.register_rule(ValidationRule(
            name="sick_animals_not_exceed_total",
            check_fn=sick_vs_total_rule,
            description="Sick animals should not exceed total animals",
            severity=ValidationSeverity.ERROR,
            columns=["sick_animals", "total_animals"],
        ))

        def deceased_vs_sick_rule(df: pd.DataFrame) -> Tuple[bool, str]:
            if 'deceased_animals' in df.columns and 'sick_animals' in df.columns:
                invalid = df[df['deceased_animals'] > df['sick_animals']]
                if len(invalid) > 0:
                    return False, f"Deceased exceed sick in {len(invalid)} rows"
            return True, "Deceased animals validation passed"

        self.register_rule(ValidationRule(
            name="deceased_not_exceed_sick",
            check_fn=deceased_vs_sick_rule,
            description="Deceased animals should not exceed sick animals",
            severity=ValidationSeverity.ERROR,
            columns=["deceased_animals", "sick_animals"],
        ))

    def register_rule(self, rule: ValidationRule) -> None:
        self.custom_rules[rule.name] = rule
        logger.info(f"Registered validation rule: {rule.name}")

    def validate_with_schema(
        self, schema_name: str, data: pd.DataFrame, apply_custom_rules: bool = True
    ) -> Dict[str, Any]:
        """Validate data against a schema with optional custom rules."""
        schema = self.schema_registry.get_schema(schema_name)
        if not schema:
            raise ValueError(f"Schema '{schema_name}' not found")

        schema_report = schema.validate_dataframe(data)

        report: Dict[str, Any] = {
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
                'overall_is_valid': schema_report['is_valid'],
            },
        }

        if apply_custom_rules:
            rule_results = {}

            for rule_name, rule in self.custom_rules.items():
                if rule.columns:
                    missing = [c for c in rule.columns if c not in data.columns]
                    if missing:
                        rule_results[rule_name] = {
                            'passed': True,
                            'message': f"Skipped - missing columns: {missing}",
                            'severity': 'info',
                        }
                        continue
                try:
                    passed, message = rule.check_fn(data)
                    rule_results[rule_name] = {
                        'passed': passed,
                        'message': message,
                        'severity': rule.severity.value,
                    }
                    if not passed:
                        if rule.severity == ValidationSeverity.ERROR:
                            report['summary']['custom_rule_errors'] += 1
                            report['summary']['overall_is_valid'] = False
                        elif rule.severity == ValidationSeverity.WARNING:
                            report['summary']['custom_rule_warnings'] += 1
                except Exception as e:
                    rule_results[rule_name] = {
                        'passed': False,
                        'message': f"Rule execution failed: {e}",
                        'severity': 'error',
                    }
                    report['summary']['custom_rule_errors'] += 1
                    report['summary']['overall_is_valid'] = False

            # Duplicate primary-key rule — only runs if all key columns exist
            if schema.primary_key:
                missing_pk = [c for c in schema.primary_key if c not in data.columns]
                if missing_pk:
                    rule_results['duplicate_primary_key'] = {
                        'passed': True,
                        'message': f"Skipped - missing primary key columns: {missing_pk}",
                        'severity': 'info',
                    }
                else:
                    duplicates = data.duplicated(subset=schema.primary_key, keep=False)
                    passed = not duplicates.any()
                    rule_results['duplicate_primary_key'] = {
                        'passed': passed,
                        'message': (
                            "No duplicate primary keys found" if passed
                            else f"Found {duplicates.sum()} duplicate rows"
                        ),
                        'severity': 'error',
                    }
                    if not passed:
                        report['summary']['custom_rule_errors'] += 1
                        report['summary']['overall_is_valid'] = False

            report['custom_rules'] = rule_results

        report['is_valid'] = report['summary']['overall_is_valid']
        logger.info(f"Validation complete: {'VALID' if report['is_valid'] else 'INVALID'}")
        return report

    def validate_batch(self, data_batch: List[Dict], schema_name: str) -> Dict[str, Any]:
        if not data_batch:
            return {'is_valid': True, 'message': 'Empty batch',
                    'valid_count': 0, 'invalid_count': 0}
        df = pd.DataFrame(data_batch)
        return self.validate_with_schema(schema_name, df)

    def create_data_quality_report(
        self, data: pd.DataFrame, schema_name: str
    ) -> Dict[str, Any]:
        """Create comprehensive data quality report."""
        validation_report = self.validate_with_schema(schema_name, data)

        quality_metrics = {
            'completeness': self._calc_completeness(data),
            'accuracy': self._calc_accuracy(data, validation_report),
            'consistency': self._calc_consistency(data),
            'timeliness': self._calc_timeliness(data),
            'validity': validation_report['summary']['overall_is_valid'],
        }

        weights = {
            'completeness': 0.3, 'accuracy': 0.3, 'consistency': 0.2,
            'timeliness': 0.1, 'validity': 0.1,
        }

        numeric_score = 0.0
        for metric in ['completeness', 'accuracy', 'consistency', 'timeliness']:
            numeric_score += quality_metrics[metric] * weights[metric]
        numeric_score += (1.0 if quality_metrics['validity'] else 0.0) * weights['validity']

        return {
            'timestamp': datetime.now().isoformat(),
            'data_shape': {'rows': len(data), 'columns': len(data.columns)},
            'quality_metrics': quality_metrics,
            'quality_score': round(numeric_score, 3),
            'quality_grade': self._grade(numeric_score),
            'validation_summary': validation_report['summary'],
        }

    def _calc_completeness(self, data: pd.DataFrame) -> float:
        if len(data) == 0:
            return 0.0
        total = data.size
        filled = data.count().sum()
        return float(filled / total) if total else 0.0

    def _calc_accuracy(self, data: pd.DataFrame, report: Dict) -> float:
        total = len(data)
        if total == 0:
            return 0.0
        error_rows = 0
        for rv in report['schema_validation'].get('row_validation', []):
            if rv.get('errors'):
                error_rows += 1
        return float((total - error_rows) / total)

    def _calc_consistency(self, data: pd.DataFrame) -> float:
        score = 1.0
        if 'sick_animals' in data.columns and 'total_animals' in data.columns:
            if (data['sick_animals'] > data['total_animals']).any():
                score -= 0.2
        if 'deceased_animals' in data.columns and 'sick_animals' in data.columns:
            if (data['deceased_animals'] > data['sick_animals']).any():
                score -= 0.2
        return max(0.0, score)

    def _calc_timeliness(self, data: pd.DataFrame) -> float:
        date_col = 'date' if 'date' in data.columns else (
            'timestamp' if 'timestamp' in data.columns else None
        )
        if not date_col:
            return 0.5
        try:
            series = data[date_col]
            if not pd.api.types.is_datetime64_any_dtype(series):
                series = pd.to_datetime(series, errors='coerce')
            most_recent = series.max()
            if pd.isna(most_recent):
                return 0.5
            days_old = (datetime.now() - most_recent.to_pydatetime()).days
            if days_old <= 1:
                return 1.0
            if days_old <= 3:
                return 0.8
            if days_old <= 7:
                return 0.5
            return 0.2
        except Exception:
            return 0.5

    def _grade(self, score: float) -> str:
        if score >= 0.9: return "A"
        if score >= 0.8: return "B"
        if score >= 0.7: return "C"
        if score >= 0.6: return "D"
        return "F"

    def save_validation_report(self, report: Dict, filepath: str) -> None:
        try:
            with open(filepath, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Validation report saved to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save validation report: {e}")


_data_validator: Optional[DataValidator] = None


def get_data_validator(config: Optional[Dict] = None) -> DataValidator:
    global _data_validator
    if _data_validator is None:
        _data_validator = DataValidator(config)
    return _data_validator


def reset_data_validator() -> None:
    global _data_validator
    _data_validator = None