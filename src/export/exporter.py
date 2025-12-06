"""
Data exporter for multiple formats
"""

import pandas as pd
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Union
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataExporter:
    """Export data in multiple formats (CSV, Excel, JSON)"""
    
    def __init__(self, output_dir: str = './outputs/exports'):
        """
        Initialize exporter
        
        Args:
            output_dir: Directory to save exported files
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def export_dataframe(self, 
                        df: pd.DataFrame,
                        filename: str,
                        formats: List[str] = None) -> Dict[str, str]:
        """
        Export dataframe to multiple formats
        
        Args:
            df: DataFrame to export
            filename: Base filename (without extension)
            formats: List of formats ['csv', 'excel', 'json']
            
        Returns:
            Dictionary with format: filepath pairs
        """
        if formats is None:
            formats = ['csv', 'excel', 'json']
        
        if df.empty:
            logger.warning("Cannot export empty dataframe")
            return {}
        
        # Add timestamp to filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_filename = f"{filename}_{timestamp}"
        
        exported_files = {}
        
        for fmt in formats:
            if fmt == 'csv':
                filepath = self._export_to_csv(df, base_filename)
                exported_files['csv'] = filepath
                
            elif fmt == 'excel':
                filepath = self._export_to_excel(df, base_filename)
                exported_files['excel'] = filepath
                
            elif fmt == 'json':
                filepath = self._export_to_json(df, base_filename)
                exported_files['json'] = filepath
        
        logger.info(f"Exported {len(df)} records to {', '.join(exported_files.keys())}")
        return exported_files
    
    def _export_to_csv(self, df: pd.DataFrame, base_filename: str) -> str:
        """Export dataframe to CSV"""
        filepath = os.path.join(self.output_dir, f"{base_filename}.csv")
        df.to_csv(filepath, index=False, encoding='utf-8')
        logger.debug(f"Exported CSV to {filepath}")
        return filepath
    
    def _export_to_excel(self, df: pd.DataFrame, base_filename: str) -> str:
        """Export dataframe to Excel"""
        filepath = os.path.join(self.output_dir, f"{base_filename}.xlsx")
        
        # Create Excel writer
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            # Main data sheet
            df.to_excel(writer, sheet_name='Data', index=False)
            
            # Add summary sheet if dataframe is not too large
            if len(df) > 0:
                self._add_excel_summary(writer, df)
        
        logger.debug(f"Exported Excel to {filepath}")
        return filepath
    
    def _add_excel_summary(self, writer, df: pd.DataFrame):
        """Add summary sheet to Excel file"""
        try:
            # Create summary statistics
            summary_data = []
            
            # Basic stats
            summary_data.append(['Summary Statistics', ''])
            summary_data.append(['Total Records', len(df)])
            
            # Column statistics for numeric columns
            numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
            
            for col in numeric_cols[:10]:  # Limit to first 10 numeric columns
                if col in df.columns:
                    summary_data.append(['', ''])
                    summary_data.append([f'{col} Statistics', ''])
                    summary_data.append(['Mean', df[col].mean()])
                    summary_data.append(['Std Dev', df[col].std()])
                    summary_data.append(['Min', df[col].min()])
                    summary_data.append(['Max', df[col].max()])
                    summary_data.append(['Non-Null', df[col].count()])
            
            # Create summary dataframe
            summary_df = pd.DataFrame(summary_data, columns=['Metric', 'Value'])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
        except Exception as e:
            logger.warning(f"Could not add Excel summary: {str(e)}")
    
    def _export_to_json(self, df: pd.DataFrame, base_filename: str) -> str:
        """Export dataframe to JSON"""
        filepath = os.path.join(self.output_dir, f"{base_filename}.json")
        
        # Convert to records format
        records = df.to_dict('records')
        
        # Add metadata
        export_data = {
            'metadata': {
                'export_date': datetime.now().isoformat(),
                'record_count': len(df),
                'columns': list(df.columns),
                'data_types': {col: str(dtype) for col, dtype in df.dtypes.items()}
            },
            'data': records
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        logger.debug(f"Exported JSON to {filepath}")
        return filepath
    
    def export_anomalies(self, 
                        anomalies_df: pd.DataFrame,
                        include_detection_details: bool = True) -> Dict[str, str]:
        """
        Export anomaly data
        
        Args:
            anomalies_df: DataFrame with anomaly data
            include_detection_details: Include detection method details
            
        Returns:
            Dictionary of exported files
        """
        if anomalies_df.empty:
            return {}
        
        # Prepare data for export
        export_df = anomalies_df.copy()
        
        # Filter to only anomaly columns if needed
        if include_detection_details:
            # Keep all columns
            pass
        else:
            # Keep only essential columns
            essential_cols = ['tag_id', 'date', 'animal_type', 'farm_id', 
                            'temperature', 'heart_rate', 'activity_level',
                            'is_anomaly', 'anomaly_score']
            available_cols = [col for col in essential_cols if col in export_df.columns]
            export_df = export_df[available_cols]
        
        # Export
        return self.export_dataframe(
            export_df, 
            'anomaly_report',
            formats=['csv', 'excel']
        )
    
    def export_alerts(self, alerts_data: Union[pd.DataFrame, List[Dict]]) -> Dict[str, str]:
        """
        Export alert data
        
        Args:
            alerts_data: DataFrame or list of alert dictionaries
            
        Returns:
            Dictionary of exported files
        """
        if isinstance(alerts_data, list):
            if not alerts_data:
                return {}
            df = pd.DataFrame(alerts_data)
        else:
            df = alerts_data.copy()
        
        if df.empty:
            return {}
        
        # Export
        return self.export_dataframe(
            df,
            'outbreak_alerts',
            formats=['csv', 'excel', 'json']
        )
    
    def export_health_metrics(self, 
                             metrics_df: pd.DataFrame,
                             time_range: str = None) -> Dict[str, str]:
        """
        Export health metrics data
        
        Args:
            metrics_df: DataFrame with health metrics
            time_range: Optional time range description
            
        Returns:
            Dictionary of exported files
        """
        if metrics_df.empty:
            return {}
        
        # Prepare filename
        filename = 'health_metrics'
        if time_range:
            filename = f"{filename}_{time_range}"
        
        # Export
        return self.export_dataframe(
            metrics_df,
            filename,
            formats=['csv', 'excel']
        )
    
    def generate_summary_report(self, 
                               anomalies_df: pd.DataFrame,
                               alerts: List[Dict],
                               output_format: str = 'txt') -> str:
        """
        Generate human-readable summary report
        
        Args:
            anomalies_df: DataFrame with anomalies
            alerts: List of alert dictionaries
            output_format: 'txt' or 'md' (markdown)
            
        Returns:
            Filepath to generated report
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if output_format == 'txt':
            filepath = os.path.join(self.output_dir, f"summary_report_{timestamp}.txt")
            content = self._generate_text_report(anomalies_df, alerts)
        elif output_format == 'md':
            filepath = os.path.join(self.output_dir, f"summary_report_{timestamp}.md")
            content = self._generate_markdown_report(anomalies_df, alerts)
        else:
            raise ValueError(f"Unsupported format: {output_format}")
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"Generated {output_format} report: {filepath}")
        return filepath
    
    def _generate_text_report(self, anomalies_df: pd.DataFrame, alerts: List[Dict]) -> str:
        """Generate text summary report"""
        lines = []
        
        lines.append("=" * 60)
        lines.append("LIVESTOCK HEALTH MONITORING SUMMARY REPORT")
        lines.append("=" * 60)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        # Anomaly Summary
        if not anomalies_df.empty:
            total_records = len(anomalies_df)
            anomaly_count = anomalies_df['is_anomaly'].sum() if 'is_anomaly' in anomalies_df.columns else 0
            
            lines.append("ANOMALY SUMMARY")
            lines.append("-" * 40)
            lines.append(f"Total Records Analyzed: {total_records}")
            lines.append(f"Anomalies Detected: {anomaly_count}")
            
            if total_records > 0:
                anomaly_rate = (anomaly_count / total_records) * 100
                lines.append(f"Anomaly Rate: {anomaly_rate:.1f}%")
            
            # By animal type
            if 'animal_type' in anomalies_df.columns:
                lines.append("")
                lines.append("Anomalies by Animal Type:")
                for animal_type, group in anomalies_df.groupby('animal_type'):
                    count = group['is_anomaly'].sum() if 'is_anomaly' in group.columns else 0
                    lines.append(f"  {animal_type.title()}: {count}")
        
        # Alert Summary
        if alerts:
            lines.append("")
            lines.append("OUTBREAK ALERTS")
            lines.append("-" * 40)
            
            for i, alert in enumerate(alerts, 1):
                lines.append(f"Alert {i}:")
                lines.append(f"  Farm: {alert.get('farm_id', 'Unknown')}")
                lines.append(f"  Severity: {alert.get('severity', 'Unknown')}")
                lines.append(f"  Affected Animals: {alert.get('affected_animals', 0)}")
                
                if 'start_date' in alert:
                    start_date = alert['start_date']
                    if hasattr(start_date, 'strftime'):
                        start_date = start_date.strftime('%Y-%m-%d')
                    lines.append(f"  Start Date: {start_date}")
        
        else:
            lines.append("")
            lines.append("No outbreak alerts generated.")
        
        # Recommendations
        lines.append("")
        lines.append("RECOMMENDATIONS")
        lines.append("-" * 40)
        
        if alerts:
            lines.append("1. Review all outbreak alerts")
            lines.append("2. Isolate affected animals if not already done")
            lines.append("3. Increase monitoring frequency")
            lines.append("4. Consult with veterinary services")
        elif anomaly_count > 10:
            lines.append("1. Monitor individual anomalies closely")
            lines.append("2. Check environmental conditions")
            lines.append("3. Review feeding schedules")
        else:
            lines.append("1. Continue regular monitoring")
            lines.append("2. Maintain current biosecurity measures")
        
        lines.append("")
        lines.append("=" * 60)
        lines.append("Report generated by Livestock Outbreak Detection System")
        
        return "\n".join(lines)
    
    def _generate_markdown_report(self, anomalies_df: pd.DataFrame, alerts: List[Dict]) -> str:
        """Generate markdown summary report"""
        lines = []
        
        lines.append("# Livestock Health Monitoring Summary Report")
        lines.append("")
        lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        # Anomaly Summary
        if not anomalies_df.empty:
            total_records = len(anomalies_df)
            anomaly_count = anomalies_df['is_anomaly'].sum() if 'is_anomaly' in anomalies_df.columns else 0
            
            lines.append("## Anomaly Summary")
            lines.append("")
            lines.append(f"- **Total Records Analyzed:** {total_records}")
            lines.append(f"- **Anomalies Detected:** {anomaly_count}")
            
            if total_records > 0:
                anomaly_rate = (anomaly_count / total_records) * 100
                lines.append(f"- **Anomaly Rate:** {anomaly_rate:.1f}%")
            
            # By animal type
            if 'animal_type' in anomalies_df.columns:
                lines.append("")
                lines.append("### Anomalies by Animal Type")
                lines.append("")
                for animal_type, group in anomalies_df.groupby('animal_type'):
                    count = group['is_anomaly'].sum() if 'is_anomaly' in group.columns else 0
                    lines.append(f"- **{animal_type.title()}:** {count}")
        
        # Alert Summary
        if alerts:
            lines.append("")
            lines.append("## Outbreak Alerts")
            lines.append("")
            
            for i, alert in enumerate(alerts, 1):
                severity = alert.get('severity', 'Unknown')
                severity_emoji = {
                    'critical': '🔴',
                    'high': '🟠',
                    'medium': '🟡',
                    'low': '🟢'
                }.get(severity.lower(), '⚪')
                
                lines.append(f"### {severity_emoji} Alert {i}: {severity.upper()}")
                lines.append("")
                lines.append(f"- **Farm:** {alert.get('farm_id', 'Unknown')}")
                lines.append(f"- **Severity:** {severity}")
                lines.append(f"- **Affected Animals:** {alert.get('affected_animals', 0)}")
                
                if 'start_date' in alert:
                    start_date = alert['start_date']
                    if hasattr(start_date, 'strftime'):
                        start_date = start_date.strftime('%Y-%m-%d')
                    lines.append(f"- **Start Date:** {start_date}")
                
                if 'description' in alert:
                    lines.append(f"- **Description:** {alert['description']}")
                
                lines.append("")
        
        else:
            lines.append("")
            lines.append("## Outbreak Alerts")
            lines.append("")
            lines.append("No outbreak alerts generated.")
        
        # Recommendations
        lines.append("")
        lines.append("## Recommendations")
        lines.append("")
        
        if alerts:
            lines.append("1. 🔍 **Review all outbreak alerts**")
            lines.append("2. 🚫 **Isolate affected animals** if not already done")
            lines.append("3. 📈 **Increase monitoring frequency**")
            lines.append("4. 🩺 **Consult with veterinary services**")
        elif anomaly_count > 10:
            lines.append("1. 👁️ **Monitor individual anomalies closely**")
            lines.append("2. 🌡️ **Check environmental conditions**")
            lines.append("3. 🍽️ **Review feeding schedules**")
        else:
            lines.append("1. ✅ **Continue regular monitoring**")
            lines.append("2. 🛡️ **Maintain current biosecurity measures**")
        
        lines.append("")
        lines.append("---")
        lines.append("*Report generated by Livestock Outbreak Detection System*")
        
        return "\n".join(lines)
    
    def list_exports(self, days: int = 7) -> List[Dict]:
        """
        List exported files from the last N days
        
        Args:
            days: Number of days to look back
            
        Returns:
            List of export file information
        """
        if not os.path.exists(self.output_dir):
            return []
        
        cutoff_time = datetime.now().timestamp() - (days * 24 * 60 * 60)
        
        exports = []
        for filename in os.listdir(self.output_dir):
            filepath = os.path.join(self.output_dir, filename)
            
            # Skip directories
            if not os.path.isfile(filepath):
                continue
            
            # Check file age
            file_mtime = os.path.getmtime(filepath)
            if file_mtime < cutoff_time:
                continue
            
            # Get file info
            file_size = os.path.getsize(filepath)
            file_ext = os.path.splitext(filename)[1].lower()
            
            exports.append({
                'filename': filename,
                'filepath': filepath,
                'size': file_size,
                'extension': file_ext,
                'modified': datetime.fromtimestamp(file_mtime).isoformat(),
                'size_human': self._human_readable_size(file_size)
            })
        
        # Sort by modification time (newest first)
        exports.sort(key=lambda x: x['modified'], reverse=True)
        
        return exports
    
    def _human_readable_size(self, size_bytes: int) -> str:
        """Convert bytes to human readable size"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"
    
    def cleanup_old_exports(self, days_to_keep: int = 30) -> int:
        """
        Delete export files older than specified days
        
        Args:
            days_to_keep: Keep files newer than this many days
            
        Returns:
            Number of files deleted
        """
        if not os.path.exists(self.output_dir):
            return 0
        
        cutoff_time = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)
        deleted_count = 0
        
        for filename in os.listdir(self.output_dir):
            filepath = os.path.join(self.output_dir, filename)
            
            if os.path.isfile(filepath):
                file_mtime = os.path.getmtime(filepath)
                
                if file_mtime < cutoff_time:
                    try:
                        os.remove(filepath)
                        deleted_count += 1
                        logger.info(f"Deleted old export: {filename}")
                    except Exception as e:
                        logger.warning(f"Failed to delete {filename}: {str(e)}")
        
        return deleted_count