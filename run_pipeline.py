"""
Main pipeline runner with feature toggle support
"""
import logging
import yaml
from typing import Dict, Any
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from config_manager.manager import get_config_manager
from utils.feature_manager import get_feature_manager, FeatureDisabledError
from data_collection.ingestion import DataCollector
from data_quality.analyzer import DataQualityAnalyzer
from preprocessing.cleaner import DataCleaner
from preprocessing.normalizer import DataNormalizer
from anomaly_detection.detector import AnomalyDetector
from anomaly_detection.ensemble import EnsembleDetector
from notification.manager import NotificationManager
from export.exporter import ReportExporter
from visualization.dashboard import Dashboard

from data_validation.validator import get_data_validator
from data_validation.schema import get_schema_registry

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FeatureAwarePipeline:
    def __init__(self, config_path: str = "config/settings.yaml", env: str = None):
        # Get config manager
        self.config_manager = get_config_manager("config", env)
        self.config = self.config_manager.config
        
        # Get environment info
        from config_manager.environments import get_environment_manager
        self.env_manager = get_environment_manager()
        
        # Get secrets manager
        from config_manager.secrets import get_secrets_manager
        self.secrets_manager = get_secrets_manager()
        
        # Rest of initialization...
        
        logger.info(f"Pipeline initialized for {self.env_manager.current_env.value} environment")
    
    def __init__(self, config_path: str = "config/settings.yaml"):
        """Initialize pipeline with configuration"""
        self.config = load_config(config_path)
        self.feature_manager = get_feature_manager(self.config)
        self.components = {}

        # Initialize data validator if enabled
        self.data_validator = None
        validation_config = self.config.get('validation', {})
        if validation_config.get('enabled', False):
        self.data_validator = get_data_validator(self.config)
        logger.info("Data validation enabled")
                
        logger.info("Pipeline initialized with feature toggles")
        self._log_feature_status()
    
    def _log_feature_status(self) -> None:
        """Log the status of all features"""
        status = self.feature_manager.get_feature_status()
        enabled = [name for name, info in status.items() if info['enabled']]
        disabled = [name for name, info in status.items() if not info['enabled']]
        
        logger.info(f"Enabled features: {len(enabled)}")
        logger.info(f"Disabled features: {len(disabled)}")
        
        if disabled:
            logger.debug(f"Disabled: {', '.join(disabled)}")
    
    def initialize_components(self) -> None:
        """Initialize pipeline components based on enabled features"""
        
        # Data collection
        if self.feature_manager.is_enabled('data_collection'):
            from data_collection.ingestion import DataCollector
            self.components['data_collector'] = DataCollector(self.config)
            logger.info("Data collector initialized")
        else:
            logger.info("Data collection feature is disabled")
        
        # Data quality
        if self.feature_manager.is_enabled('data_quality'):
            from data_quality.analyzer import DataQualityAnalyzer
            self.components['data_quality'] = DataQualityAnalyzer(self.config)
            logger.info("Data quality analyzer initialized")
        
        # Preprocessing
        if self.feature_manager.is_enabled('preprocessing'):
            from preprocessing.cleaner import DataCleaner
            from preprocessing.normalizer import DataNormalizer
            self.components['cleaner'] = DataCleaner(self.config)
            self.components['normalizer'] = DataNormalizer(self.config)
            logger.info("Preprocessing components initialized")
        
        # Anomaly detection
        if self.feature_manager.is_enabled('anomaly_detection'):
            from anomaly_detection.detector import AnomalyDetector
            self.components['detector'] = AnomalyDetector(self.config)
            logger.info("Anomaly detector initialized")
        
        # Ensemble detection
        if self.feature_manager.is_enabled('ensemble_detection'):
            from anomaly_detection.ensemble import EnsembleDetector
            self.components['ensemble'] = EnsembleDetector(self.config)
            logger.info("Ensemble detector initialized")
        
        # Notifications
        if self.feature_manager.is_enabled('notifications'):
            from notification.manager import NotificationManager
            self.components['notifications'] = NotificationManager(self.config)
            logger.info("Notification manager initialized")
        
        # Export
        if self.feature_manager.is_enabled('export_reports'):
            from export.exporter import ReportExporter
            self.components['exporter'] = ReportExporter(self.config)
            logger.info("Report exporter initialized")
        
        # Dashboard
        if self.feature_manager.is_enabled('dashboard'):
            from visualization.dashboard import Dashboard
            self.components['dashboard'] = Dashboard(self.config)
            logger.info("Dashboard initialized")
    
    def run(self, input_data=None) -> Dict[str, Any]:
        """
        Run the pipeline with feature awareness
        
        Args:
            input_data: Optional input data (if data_collection is disabled)
            
        Returns:
            Dictionary with pipeline results
        """
        results = {
            'success': False,
            'features_used': [],
            'warnings': [],
            'errors': []
        }
        
        try:
            # Step 1: Collect data (if enabled)
            if self.feature_manager.is_enabled('data_collection'):
                logger.info("Collecting data...")
                data = self.components['data_collector'].collect()
                results['features_used'].append('data_collection')
            elif input_data is not None:
                logger.info("Using provided input data")
                data = input_data
            else:
                raise FeatureDisabledError(
                    "data_collection is disabled and no input data provided"
                )
            
            # Step 2: Data quality checks (if enabled)
            if self.feature_manager.is_enabled('data_quality'):
                logger.info("Running data quality checks...")
                quality_report = self.components['data_quality'].analyze(data)
                results['data_quality'] = quality_report
                results['features_used'].append('data_quality')
                
                # Log quality issues
                if quality_report.get('has_issues', False):
                    results['warnings'].append('Data quality issues detected')
            
            # Step 3: Preprocessing (if enabled)
            if self.feature_manager.is_enabled('preprocessing'):
                logger.info("Preprocessing data...")
                # Clean
                if 'cleaner' in self.components:
                    data = self.components['cleaner'].clean(data)
                
                # Normalize
                if 'normalizer' in self.components:
                    data = self.components['normalizer'].normalize(data)
                
                results['features_used'].append('preprocessing')
            
            # Step 4: Anomaly detection (if enabled)
            anomalies = None
            if self.feature_manager.is_enabled('anomaly_detection'):
                logger.info("Running anomaly detection...")
                anomalies = self.components['detector'].detect(data)
                results['anomalies'] = anomalies
                results['features_used'].append('anomaly_detection')
            
            # Step 5: Ensemble detection (if enabled)
            if (self.feature_manager.is_enabled('ensemble_detection') and 
                anomalies is not None):
                logger.info("Running ensemble detection...")
                ensemble_result = self.components['ensemble'].ensemble_detect(
                    data, anomalies
                )
                results['ensemble_result'] = ensemble_result
                results['features_used'].append('ensemble_detection')
            
            # Step 6: Notifications (if enabled)
            if (self.feature_manager.is_enabled('notifications') and 
                anomalies is not None and 
                len(anomalies) > 0):
                
                logger.info("Sending notifications...")
                
                # Only send email alerts if that feature is also enabled
                if self.feature_manager.is_enabled('email_alerts'):
                    self.components['notifications'].send_email_alert(
                        anomalies, "Anomalies detected in livestock data"
                    )
                    results['features_used'].append('email_alerts')
                else:
                    # Just log, don't send email
                    self.components['notifications'].log_alert(anomalies)
                
                results['features_used'].append('notifications')
            
            # Step 7: Export reports (if enabled)
            if self.feature_manager.is_enabled('export_reports'):
                logger.info("Exporting reports...")
                export_path = self.components['exporter'].export(
                    data, anomalies, results
                )
                results['export_path'] = export_path
                results['features_used'].append('export_reports')
            
            # Step 8: Update dashboard (if enabled)
            if self.feature_manager.is_enabled('dashboard'):
                logger.info("Updating dashboard...")
                self.components['dashboard'].update(data, anomalies, results)
                results['features_used'].append('dashboard')
            
            results['success'] = True
            logger.info("Pipeline completed successfully")
            
        except FeatureDisabledError as e:
            logger.warning(f"Feature disabled: {e}")
            results['errors'].append(str(e))
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            results['errors'].append(str(e))
            raise
        
        return results
    
    def run_with_features(self, enabled_features: Dict[str, bool]) -> Dict[str, Any]:
        """
        Run pipeline with temporary feature states
        
        Args:
            enabled_features: Dictionary mapping feature names to boolean states
            
        Returns:
            Pipeline results
        """
        # Store original states
        original_states = {}
        for feature_name, enabled in enabled_features.items():
            if feature_name in self.feature_manager._features:
                original_states[feature_name] = (
                    self.feature_manager._features[feature_name].current_state
                )
                self.feature_manager._features[feature_name].current_state = enabled
        
        try:
            results = self.run()
        finally:
            # Restore original states
            for feature_name, state in original_states.items():
                self.feature_manager._features[feature_name].current_state = state
        
        return results


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run livestock outbreak detection pipeline')
    parser.add_argument('--config', default='config/settings.yaml',
                       help='Path to configuration file')
    parser.add_argument('--disable', nargs='+', default=[],
                       help='Features to disable (comma-separated)')
    parser.add_argument('--enable', nargs='+', default=[],
                       help='Features to enable (comma-separated)')
    parser.add_argument('--list-features', action='store_true',
                       help='List all available features and exit')
    
    args = parser.parse_args()
    
    # Load config
    config = load_config(args.config)
    
    # Get feature manager
    feature_manager = get_feature_manager(config)
    
    # List features if requested
    if args.list_features:
        print("\n=== Available Features ===")
        for name, feature in feature_manager.get_all_features().items():
            status = "✓" if feature_manager.is_enabled(name) else "✗"
            state = feature.state.value.upper()
            print(f"{status} {name:30} [{state:12}] - {feature.description}")
        print()
        
        # Show experimental features separately
        experimental = feature_manager.get_features_by_category('experimental')
        if experimental:
            print("=== Experimental Features ===")
            for name, feature in experimental.items():
                status = "✓" if feature_manager.is_enabled(name) else "✗"
                print(f"{status} {name:30} - {feature.description}")
        return
    
    # Modify feature states based on CLI args
    for feature in args.disable:
        if feature in feature_manager._features:
            feature_manager.disable_feature(feature)
            print(f"Disabled feature: {feature}")
        else:
            print(f"Warning: Unknown feature '{feature}'")
    
    for feature in args.enable:
        if feature in feature_manager._features:
            feature_manager.enable_feature(feature)
            print(f"Enabled feature: {feature}")
        else:
            print(f"Warning: Unknown feature '{feature}'")
    
    # Run pipeline
    pipeline = FeatureAwarePipeline(args.config)

    # Step 1.5: Data validation (if enabled)
if self.data_validator and self.feature_manager.is_enabled('data_quality'):
    logger.info("Validating data...")
    
    schema_name = self.config.get('validation', {}).get('required_schema', 'daily_health_metrics')
    
    validation_report = self.data_validator.validate_with_schema(schema_name, data)
    results['validation_report'] = validation_report
    
    # Check if data is valid
    if not validation_report['is_valid']:
        if self.config.get('validation', {}).get('strict_mode', False):
            logger.error("Data validation failed in strict mode. Aborting pipeline.")
            results['errors'].append("Data validation failed")
            results['success'] = False
            return results
        else:
            logger.warning("Data validation failed, but continuing in non-strict mode")
            results['warnings'].append("Data validation failed")
    
    # Generate quality report
    quality_report = self.data_validator.create_data_quality_report(data, schema_name)
    results['quality_report'] = quality_report
    
    logger.info(f"Data quality score: {quality_report['quality_score']:.3f}")
    
    results['features_used'].append('data_validation')
    pipeline.initialize_components()
    
    print(f"\nRunning pipeline with {len(pipeline.feature_manager.get_enabled_features())} enabled features")
    results = pipeline.run()
    
    # Print summary
    if results['success']:
        print(f"\n✅ Pipeline completed successfully!")
        print(f"   Features used: {', '.join(results['features_used'])}")
        if 'anomalies' in results:
            print(f"   Anomalies detected: {len(results['anomalies'])}")
        if results.get('warnings'):
            print(f"   Warnings: {', '.join(results['warnings'])}")
    else:
        print(f"\n❌ Pipeline failed")
        if results.get('errors'):
            print(f"   Errors: {', '.join(results['errors'])}")


if __name__ == "__main__":
    main()