#!/usr/bin/env python3
"""
Data validation CLI tool
"""
import argparse
import logging
import sys
import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from utils.config import load_config
from data_validation.validator import get_data_validator, reset_data_validator
from data_validation.schema import get_schema_registry

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ValidationCLI:
    """Command-line interface for data validation"""
    
    def __init__(self, config_path: str = "config/settings.yaml"):
        self.config = load_config(config_path)
        self.validator = get_data_validator(self.config)
        self.schema_registry = get_schema_registry()
    
    def validate_file(self, filepath: str, schema_name: str, 
                     output_report: str = None) -> bool:
        """Validate data from a file"""
        logger.info(f"Validating file: {filepath}")
        
        # Read file based on extension
        file_ext = Path(filepath).suffix.lower()
        
        try:
            if file_ext == '.csv':
                data = pd.read_csv(filepath)
            elif file_ext == '.json':
                data = pd.read_json(filepath)
            elif file_ext == '.parquet':
                data = pd.read_parquet(filepath)
            elif file_ext in ['.xlsx', '.xls']:
                data = pd.read_excel(filepath)
            else:
                logger.error(f"Unsupported file format: {file_ext}")
                return False
            
        except Exception as e:
            logger.error(f"Failed to read file {filepath}: {str(e)}")
            return False
        
        # Validate data
        report = self.validator.validate_with_schema(schema_name, data)
        
        # Display results
        self._display_validation_results(report)
        
        # Save report if requested
        if output_report:
            self.validator.save_validation_report(report, output_report)
            print(f"\nValidation report saved to: {output_report}")
        
        # Generate quality report
        quality_report = self.validator.create_data_quality_report(data, schema_name)
        print(f"\n📊 Data Quality Score: {quality_report['quality_score']:.3f} ({quality_report['quality_grade']})")
        
        return report['is_valid']
    
    def validate_dataframe(self, df: pd.DataFrame, schema_name: str) -> Dict:
        """Validate a pandas DataFrame"""
        return self.validator.validate_with_schema(schema_name, df)
    
    def list_schemas(self) -> None:
        """List all available schemas"""
        schemas = self.schema_registry.list_schemas()
        
        print("\n📋 Available Schemas:")
        print("=" * 60)
        
        for schema_name in schemas:
            schema = self.schema_registry.get_schema(schema_name)
            print(f"\n📄 {schema_name}")
            print(f"   Version: {schema.version}")
            print(f"   Description: {schema.description}")
            print(f"   Columns: {len(schema.columns)}")
            if schema.required_columns:
                print(f"   Required: {', '.join(schema.required_columns)}")
            if schema.primary_key:
                print(f"   Primary Key: {', '.join(schema.primary_key)}")
        
        print(f"\nTotal schemas: {len(schemas)}")
    
    def show_schema(self, schema_name: str) -> None:
        """Show schema details"""
        schema = self.schema_registry.get_schema(schema_name)
        if not schema:
            print(f"Schema '{schema_name}' not found")
            return
        
        print(f"\n📄 Schema: {schema.name} v{schema.version}")
        print(f"Description: {schema.description}")
        
        if schema.primary_key:
            print(f"Primary Key: {', '.join(schema.primary_key)}")
        
        if schema.row_count_range:
            min_rows, max_rows = schema.row_count_range
            print(f"Expected rows: {min_rows} - {max_rows}")
        
        print("\nColumns:")
        print("-" * 80)
        print(f"{'Name':20} {'Type':15} {'Required':10} {'Nullable':10} {'Description'}")
        print("-" * 80)
        
        for col in schema.columns:
            required = "Yes" if col.required else "No"
            nullable = "Yes" if col.nullable else "No"
            constraints = []
            
            if col.min_value is not None:
                constraints.append(f"min={col.min_value}")
            if col.max_value is not None:
                constraints.append(f"max={col.max_value}")
            if col.allowed_values:
                constraints.append(f"options={len(col.allowed_values)}")
            if col.pattern:
                constraints.append("pattern")
            
            constraint_str = f" [{', '.join(constraints)}]" if constraints else ""
            
            print(f"{col.name:20} {col.data_type.value:15} {required:10} {nullable:10} {col.description}{constraint_str}")
    
    def generate_schema_template(self, schema_name: str, output_file: str = None) -> None:
        """Generate a template file for a schema"""
        schema = self.schema_registry.get_schema(schema_name)
        if not schema:
            print(f"Schema '{schema_name}' not found")
            return
        
        # Create template DataFrame
        template_data = {}
        for col in schema.columns:
            # Set example values based on data type
            if col.data_type.value == 'integer':
                example = 0
            elif col.data_type.value == 'float':
                example = 0.0
            elif col.data_type.value == 'string':
                example = "example"
            elif col.data_type.value == 'boolean':
                example = True
            elif col.data_type.value == 'datetime':
                example = "2024-01-01"
            elif col.data_type.value == 'percentage':
                example = 50.0
            elif col.data_type.value == 'temperature':
                example = 37.5
            else:
                example = ""
            
            template_data[col.name] = [example]
        
        df_template = pd.DataFrame(template_data)
        
        if output_file:
            file_ext = Path(output_file).suffix.lower()
            if file_ext == '.csv':
                df_template.to_csv(output_file, index=False)
            elif file_ext == '.json':
                df_template.to_json(output_file, orient='records', indent=2)
            elif file_ext == '.xlsx':
                df_template.to_excel(output_file, index=False)
            else:
                df_template.to_csv(output_file, index=False)
            
            print(f"Schema template saved to: {output_file}")
        else:
            # Print as CSV
            print(df_template.to_csv(index=False))
    
    def check_quality(self, filepath: str, schema_name: str) -> None:
        """Check data quality"""
        logger.info(f"Checking data quality for: {filepath}")
        
        # Read file
        file_ext = Path(filepath).suffix.lower()
        
        try:
            if file_ext == '.csv':
                data = pd.read_csv(filepath)
            elif file_ext == '.json':
                data = pd.read_json(filepath)
            else:
                print(f"Unsupported file format: {file_ext}")
                return
        except Exception as e:
            print(f"Failed to read file: {str(e)}")
            return
        
        # Generate quality report
        quality_report = self.validator.create_data_quality_report(data, schema_name)
        
        print("\n" + "=" * 60)
        print("📊 DATA QUALITY REPORT")
        print("=" * 60)
        
        # Quality grade with color
        grade = quality_report['quality_grade']
        score = quality_report['quality_score']
        
        if grade == 'A':
            grade_display = f"🟢 {grade}"
        elif grade == 'B':
            grade_display = f"🟡 {grade}"
        elif grade == 'C':
            grade_display = f"🟠 {grade}"
        else:
            grade_display = f"🔴 {grade}"
        
        print(f"\nOverall Quality: {grade_display} ({score:.3f})")
        
        # Metrics
        print("\nQuality Metrics:")
        print("-" * 40)
        for metric, value in quality_report['quality_metrics'].items():
            if isinstance(value, float):
                print(f"  {metric.capitalize():15}: {value:.3f}")
            else:
                print(f"  {metric.capitalize():15}: {value}")
        
        # Data shape
        print(f"\nData Shape: {quality_report['data_shape']['rows']} rows × {quality_report['data_shape']['columns']} columns")
        
        # Major issues
        if quality_report['major_issues']:
            print("\n⚠️  Major Issues:")
            for issue in quality_report['major_issues']:
                print(f"  • {issue['description']} ({issue['count']} occurrences)")
        
        # Recommendations
        if quality_report['recommendations']:
            print("\n💡 Recommendations:")
            for rec in quality_report['recommendations']:
                print(f"  • {rec}")
        
        print("\n" + "=" * 60)
    
    def _display_validation_results(self, report: Dict) -> None:
        """Display validation results in a readable format"""
        print("\n" + "=" * 60)
        print("✅ DATA VALIDATION REPORT")
        print("=" * 60)
        
        # Overall status
        status = "🟢 VALID" if report['is_valid'] else "🔴 INVALID"
        print(f"\nStatus: {status}")
        print(f"Schema: {report['schema_name']} v{report['schema_version']}")
        print(f"Timestamp: {report['timestamp']}")
        
        # Summary
        summary = report['summary']
        print(f"\nRows processed: {summary['total_rows']}")
        print(f"Schema errors: {summary['schema_errors']}")
        print(f"Schema warnings: {summary['schema_warnings']}")
        print(f"Custom rule errors: {summary['custom_rule_errors']}")
        print(f"Custom rule warnings: {summary['custom_rule_warnings']}")
        
        # Column statistics
        if 'column_stats' in report['schema_validation']:
            print("\n📈 Column Statistics:")
            print("-" * 40)
            
            for col_name, stats in report['schema_validation']['column_stats'].items():
                if stats.get('error_count', 0) > 0 or stats.get('null_count', 0) > 0:
                    errors = stats.get('error_count', 0)
                    nulls = stats.get('null_count', 0)
                    print(f"  {col_name:20} - Errors: {errors:3} | Nulls: {nulls:3}")
        
        # Custom rule results
        if report['custom_rules']:
            print("\n🔧 Custom Rule Results:")
            print("-" * 40)
            
            for rule_name, result in report['custom_rules'].items():
                status = "✅" if result['passed'] else "❌"
                severity = result['severity'].upper()
                print(f"  {status} {rule_name:30} [{severity:8}] - {result['message']}")
        
        print("\n" + "=" * 60)
    
    def interactive_validation(self) -> None:
        """Interactive validation mode"""
        print("\n" + "=" * 60)
        print("🔍 INTERACTIVE DATA VALIDATION")
        print("=" * 60)
        
        # List available schemas
        self.list_schemas()
        
        while True:
            print("\nOptions:")
            print("  1. Validate a data file")
            print("  2. Check data quality")
            print("  3. View schema details")
            print("  4. Generate schema template")
            print("  5. Exit")
            
            choice = input("\nEnter choice (1-5): ").strip()
            
            if choice == '1':
                filepath = input("Enter file path: ").strip()
                schema_name = input("Enter schema name: ").strip()
                
                if os.path.exists(filepath):
                    output_report = input("Output report file (optional): ").strip()
                    if not output_report:
                        output_report = None
                    
                    self.validate_file(filepath, schema_name, output_report)
                else:
                    print(f"File not found: {filepath}")
            
            elif choice == '2':
                filepath = input("Enter file path: ").strip()
                schema_name = input("Enter schema name: ").strip()
                
                if os.path.exists(filepath):
                    self.check_quality(filepath, schema_name)
                else:
                    print(f"File not found: {filepath}")
            
            elif choice == '3':
                schema_name = input("Enter schema name: ").strip()
                self.show_schema(schema_name)
            
            elif choice == '4':
                schema_name = input("Enter schema name: ").strip()
                output_file = input("Output file (optional): ").strip()
                if not output_file:
                    output_file = None
                self.generate_schema_template(schema_name, output_file)
            
            elif choice == '5':
                print("Exiting interactive mode.")
                break
            
            else:
                print("Invalid choice. Please try again.")


def main():
    parser = argparse.ArgumentParser(
        description='Livestock Outbreak Detection - Data Validation Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s validate data.csv --schema daily_health_metrics
  %(prog)s schemas list
  %(prog)s schema show daily_health_metrics
  %(prog)s quality data.csv --schema daily_health_metrics
  %(prog)s interactive
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate a data file')
    validate_parser.add_argument('file', help='Data file to validate')
    validate_parser.add_argument('--schema', required=True, help='Schema name to validate against')
    validate_parser.add_argument('--output', help='Output report file (optional)')
    
    # Quality command
    quality_parser = subparsers.add_parser('quality', help='Check data quality')
    quality_parser.add_argument('file', help='Data file to check')
    quality_parser.add_argument('--schema', required=True, help='Schema name')
    
    # Schemas command
    schemas_parser = subparsers.add_parser('schemas', help='Manage schemas')
    schemas_subparsers = schemas_parser.add_subparsers(dest='schemas_command')
    schemas_subparsers.add_parser('list', help='List all schemas')
    
    # Schema command
    schema_parser = subparsers.add_parser('schema', help='Schema operations')
    schema_subparsers = schema_parser.add_subparsers(dest='schema_command')
    schema_subparsers.add_parser('list', help='List all schemas')
    show_parser = schema_subparsers.add_parser('show', help='Show schema details')
    show_parser.add_argument('name', help='Schema name')
    template_parser = schema_subparsers.add_parser('template', help='Generate schema template')
    template_parser.add_argument('name', help='Schema name')
    template_parser.add_argument('--output', help='Output file (optional)')
    
    # Interactive command
    subparsers.add_parser('interactive', help='Start interactive validation mode')
    
    # Global arguments
    parser.add_argument('--config', default='config/settings.yaml',
                       help='Path to config file')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Initialize validator
    validator = ValidationCLI(args.config)
    
    # Execute command
    if args.command == 'validate':
        success = validator.validate_file(args.file, args.schema, args.output)
        sys.exit(0 if success else 1)
        
    elif args.command == 'quality':
        validator.check_quality(args.file, args.schema)
        
    elif args.command == 'schemas':
        if args.schemas_command == 'list':
            validator.list_schemas()
        
    elif args.command == 'schema':
        if args.schema_command == 'list':
            validator.list_schemas()
        elif args.schema_command == 'show':
            validator.show_schema(args.name)
        elif args.schema_command == 'template':
            validator.generate_schema_template(args.name, args.output)
        
    elif args.command == 'interactive':
        validator.interactive_validation()


if __name__ == "__main__":
    main()