"""
Main pipeline runner with feature toggle support
"""
import sys
import os
from datetime import datetime
from typing import Dict, Any, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from custom_logging.structured_logger import get_structured_logger
from utils.config import Config
from utils.feature_manager import get_feature_manager, FeatureDisabledError
from data_validation.validator import get_data_validator


logger = get_structured_logger()
logger.set_context(component="pipeline", operation="livestock_outbreak_detection")


class FeatureAwarePipeline:
    """Feature-aware pipeline that runs the full livestock detection flow."""

    def __init__(self, config_path: str = "config/settings.yaml", env: Optional[str] = None):
        self.env = env or os.getenv("APP_ENV", "development")
        self.config = Config(config_path)
        self.feature_manager = get_feature_manager(self.config)
        self.components: Dict[str, Any] = {}

        self.data_validator = None
        if self.config.get('validation.enabled', False):
            self.data_validator = get_data_validator(self.config)
            logger.info("Data validation enabled")

        logger.info(f"Pipeline initialized for {self.env} environment")
        self._log_feature_status()

    def _log_feature_status(self) -> None:
        status = self.feature_manager.get_feature_status()
        enabled = [name for name, info in status.items() if info['enabled']]
        disabled = [name for name, info in status.items() if not info['enabled']]
        logger.info(f"Enabled features: {len(enabled)}")
        logger.info(f"Disabled features: {len(disabled)}")
        if disabled:
            logger.debug(f"Disabled: {', '.join(disabled)}")

    def initialize_components(self) -> None:
        if self.feature_manager.is_enabled('data_collection'):
            try:
                from data_collection.ingestion import DataIngestor
                self.components['data_ingestor'] = DataIngestor(None)
                logger.info("Data ingestor initialized")
            except Exception as e:
                logger.warning(f"Could not init data_ingestor: {e}")

        if self.feature_manager.is_enabled('data_quality'):
            try:
                from data_quality.analyzer import DataQualityAnalyzer
                self.components['data_quality'] = DataQualityAnalyzer()
                logger.info("Data quality analyzer initialized")
            except Exception as e:
                logger.warning(f"Could not init data_quality: {e}")

        if self.feature_manager.is_enabled('preprocessing'):
            try:
                from preprocessing.cleaner import DataCleaner
                from preprocessing.normalizer import FeatureNormalizer
                self.components['cleaner'] = DataCleaner(self.config)
                self.components['normalizer'] = FeatureNormalizer(method='standard')
                logger.info("Preprocessing components initialized")
            except Exception as e:
                logger.warning(f"Could not init preprocessing: {e}")

        if self.feature_manager.is_enabled('anomaly_detection'):
            try:
                from anomaly_detection.detector import AnomalyDetector
                self.components['detector'] = AnomalyDetector(self.config)
                logger.info("Anomaly detector initialized")
            except Exception as e:
                logger.warning(f"Could not init detector: {e}")

        if self.feature_manager.is_enabled('ensemble_detection'):
            try:
                from anomaly_detection.ensemble import EnsembleDetector
                self.components['ensemble'] = EnsembleDetector()
                logger.info("Ensemble detector initialized")
            except Exception as e:
                logger.warning(f"Could not init ensemble: {e}")

        if self.feature_manager.is_enabled('notifications'):
            try:
                from notification.manager import NotificationManager
                notification_config = self.config.get('notification', {}) or {}
                self.components['notifications'] = NotificationManager(notification_config)
                logger.info("Notification manager initialized")
            except Exception as e:
                logger.warning(f"Could not init notifications: {e}")

        if self.feature_manager.is_enabled('export_reports'):
            try:
                from export.exporter import DataExporter
                self.components['exporter'] = DataExporter()
                logger.info("Data exporter initialized")
            except Exception as e:
                logger.warning(f"Could not init exporter: {e}")

        if self.feature_manager.is_enabled('dashboard'):
            try:
                from visualization.dashboard import HealthDashboard
                self.components['dashboard'] = HealthDashboard()
                logger.info("Dashboard initialized")
            except Exception as e:
                logger.warning(f"Could not init dashboard: {e}")

    def _load_input_data(self, input_data: Optional[Any]):
        if input_data is not None:
            return input_data
        csv_path = "data/raw/livestock_data.csv"
        if not os.path.exists(csv_path):
            raise FileNotFoundError(
                f"No input_data and no file at {csv_path}. "
                f"Run `python generate_data.py` first."
            )
        import pandas as pd
        logger.info(f"Loading data from {csv_path}")
        return pd.read_csv(csv_path)

    def run(self, input_data: Optional[Any] = None) -> Dict[str, Any]:
        results: Dict[str, Any] = {
            'success': False,
            'features_used': [],
            'warnings': [],
            'errors': [],
        }

        try:
            # 1. Load
            if input_data is not None:
                data = input_data
                results['features_used'].append('provided_input')
            elif self.feature_manager.is_enabled('data_collection'):
                logger.info("Collecting data...")
                data = self._load_input_data(input_data)
                results['features_used'].append('data_collection')
            else:
                raise FeatureDisabledError(
                    "data_collection is disabled and no input data provided"
                )

            # 2. Validation
            if self.data_validator and self.feature_manager.is_enabled('data_quality'):
                logger.info("Validating data...")
                schema_name = self.config.get(
                    'validation.required_schema', 'daily_health_metrics'
                )
                validation_report = self.data_validator.validate_with_schema(
                    schema_name, data
                )
                results['validation_report'] = validation_report

                if not validation_report['is_valid']:
                    strict = self.config.get('validation.strict_mode', False)
                    if strict:
                        logger.error("Validation failed (strict mode). Aborting.")
                        results['errors'].append("Data validation failed")
                        return results
                    results['warnings'].append("Data validation failed")

                quality_report = self.data_validator.create_data_quality_report(
                    data, schema_name
                )
                results['quality_report'] = quality_report
                logger.info(f"Data quality score: {quality_report['quality_score']:.3f}")
                results['features_used'].append('data_validation')

            # 3. Data quality (guarded)
            if (
                self.feature_manager.is_enabled('data_quality')
                and 'data_quality' in self.components
            ):
                logger.info("Running data quality checks...")
                quality_analysis = self.components['data_quality'].analyze_dataframe(data)
                results['data_quality'] = quality_analysis
                if quality_analysis.get('issues'):
                    results['warnings'].append(
                        f"Data quality issues: {len(quality_analysis['issues'])}"
                    )
                results['features_used'].append('data_quality')

            # 4. Preprocess (guarded)
            if self.feature_manager.is_enabled('preprocessing'):
                logger.info("Preprocessing data...")
                if 'cleaner' in self.components:
                    data = self.components['cleaner'].clean_dataframe(data)
                if 'normalizer' in self.components:
                    metrics = [c for c in ['temperature', 'heart_rate', 'activity_level']
                               if c in data.columns]
                    if metrics:
                        self.components['normalizer'].fit(data, metrics)
                        data = self.components['normalizer'].transform(data, metrics)
                results['features_used'].append('preprocessing')

            # 5. Anomaly detection (guarded)
            anomalies_df = None
            if (
                self.feature_manager.is_enabled('anomaly_detection')
                and 'detector' in self.components
            ):
                logger.info("Running anomaly detection...")
                anomalies_df = self.components['detector'].detect(data)
                results['features_used'].append('anomaly_detection')
                if anomalies_df is not None and 'is_anomaly' in anomalies_df.columns:
                    anomaly_rows = anomalies_df[anomalies_df['is_anomaly']]
                    results['anomalies'] = anomaly_rows.to_dict('records')
                    logger.info(f"Detected {len(anomaly_rows)} anomalies")

            # 6. Ensemble (guarded)
            if (
                self.feature_manager.is_enabled('ensemble_detection')
                and 'ensemble' in self.components
                and anomalies_df is not None
            ):
                logger.info("Running ensemble detection...")
                metrics = [c for c in ['temperature', 'heart_rate', 'activity_level']
                           if c in data.columns]
                ensemble_result = self.components['ensemble'].detect_anomalies(
                    data, metrics
                )
                results['ensemble_result_summary'] = {
                    'total_anomalies': int(ensemble_result['is_anomaly'].sum())
                    if 'is_anomaly' in ensemble_result.columns else 0
                }
                results['features_used'].append('ensemble_detection')

            # 7. Notifications (guarded)
            if (
                self.feature_manager.is_enabled('notifications')
                and 'notifications' in self.components
                and results.get('anomalies')
            ):
                logger.info("Sending notifications...")
                alert_data = {
                    'severity': 'high',
                    'farm_id': 'pipeline_run',
                    'affected_animals': len(results['anomalies']),
                    'description': f"{len(results['anomalies'])} anomalies detected",
                }
                try:
                    notify_result = self.components['notifications'].send_outbreak_alert(
                        alert_data
                    )
                    results['notification_result'] = notify_result
                except Exception as e:
                    logger.warning(f"Notification failed: {e}")
                    results['warnings'].append(f"Notification failed: {e}")
                results['features_used'].append('notifications')

            # 8. Export data (guarded)
            if (
                self.feature_manager.is_enabled('export_reports')
                and 'exporter' in self.components
            ):
                logger.info("Exporting data...")
                try:
                    export_files = self.components['exporter'].export_dataframe(
                        data, 'pipeline_output'
                    )
                    results['export_files'] = export_files
                except Exception as e:
                    logger.warning(f"Data export failed: {e}")
                    results['warnings'].append(f"Data export failed: {e}")
                results['features_used'].append('export_reports')

            # 9. HTML report (guarded)
            if self.feature_manager.is_enabled('export_reports'):
                try:
                    from reporting.generator import get_report_generator
                    report_gen = get_report_generator()
                    metadata = {
                        'Pipeline Version': '1.0',
                        'Environment': self.env,
                        'Total Records': len(data) if data is not None else 0,
                    }
                    report_path = report_gen.generate_html_report(
                        data=data,
                        anomalies=results.get('anomalies', []),
                        metadata=metadata,
                        title=f"Anomaly Detection Report - "
                              f"{datetime.now().strftime('%Y-%m-%d')}",
                    )
                    results['report_path'] = report_path
                    logger.info(f"HTML report generated: {report_path}")
                except Exception as e:
                    logger.warning(f"HTML report failed: {e}")
                    results['warnings'].append(f"HTML report failed: {e}")

            # 10. Dashboard (guarded)
            if (
                self.feature_manager.is_enabled('dashboard')
                and 'dashboard' in self.components
            ):
                try:
                    if anomalies_df is not None:
                        html_report = self.components['dashboard'].create_summary_report(
                            anomalies_df, results.get('anomalies', [])
                        )
                        report_file = self.components['dashboard'].save_report(html_report)
                        results['dashboard_report'] = report_file
                        logger.info(f"Dashboard report saved: {report_file}")
                except Exception as e:
                    logger.warning(f"Dashboard failed: {e}")
                    results['warnings'].append(f"Dashboard failed: {e}")
                results['features_used'].append('dashboard')

            results['success'] = True
            logger.info("Pipeline completed successfully")

        except FeatureDisabledError as e:
            logger.warning(f"Feature disabled: {e}")
            results['errors'].append(str(e))
        except Exception as e:
            logger.error(f"Pipeline error: {e}", exception=e)
            results['errors'].append(str(e))
            raise

        return results


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Run livestock outbreak detection pipeline')
    parser.add_argument('--config', default='config/settings.yaml')
    parser.add_argument('--env', default=None)
    parser.add_argument('--disable', nargs='+', default=[])
    parser.add_argument('--enable', nargs='+', default=[])
    parser.add_argument('--list-features', action='store_true')

    args = parser.parse_args()
    pipeline = FeatureAwarePipeline(args.config, args.env)
    fm = pipeline.feature_manager

    if args.list_features:
        print("\n=== Available Features ===")
        for name, feature in fm.get_all_features().items():
            status = "✓" if fm.is_enabled(name) else "✗"
            print(f"{status} {name:30} [{feature.state.value:12}] - {feature.description}")
        return

    for feature in args.disable:
        if feature in fm._features:
            fm.disable_feature(feature)
            print(f"Disabled feature: {feature}")

    for feature in args.enable:
        if feature in fm._features:
            fm.enable_feature(feature)
            print(f"Enabled feature: {feature}")

    pipeline.initialize_components()
    print(f"\nRunning pipeline with {len(fm.get_enabled_features())} enabled features")
    results = pipeline.run()

    if results['success']:
        print("\n✅ Pipeline completed successfully!")
        print(f"   Features used: {', '.join(results['features_used'])}")
        if 'anomalies' in results:
            print(f"   Anomalies detected: {len(results['anomalies'])}")
        if results.get('report_path'):
            print(f"   HTML report: {results['report_path']}")
        if results.get('warnings'):
            print(f"   Warnings: {', '.join(results['warnings'])}")
    else:
        print("\n❌ Pipeline failed")
        if results.get('errors'):
            print(f"   Errors: {', '.join(results['errors'])}")


if __name__ == "__main__":
    main()