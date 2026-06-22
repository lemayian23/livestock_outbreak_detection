"""
HTML Report Generator for anomaly detection results
"""
import os
import json
import base64
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generate HTML reports from anomaly detection results"""
    
    def __init__(self, output_dir: str = "outputs/reports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def generate_html_report(self, 
                            data: pd.DataFrame,
                            anomalies: List[Dict],
                            metadata: Dict[str, Any],
                            title: str = "Anomaly Detection Report") -> str:
        """
        Generate an HTML report with tables and charts
        
        Returns:
            Path to the generated HTML file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"anomaly_report_{timestamp}.html"
        filepath = self.output_dir / filename
        
        # Basic statistics
        total_records = len(data)
        anomaly_count = len(anomalies)
        anomaly_percentage = (anomaly_count / total_records * 100) if total_records > 0 else 0
        
        # Create HTML content
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{title}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                h1 {{ color: #333; border-bottom: 2px solid #4CAF50; padding-bottom: 10px; }}
                h2 {{ color: #666; margin-top: 30px; }}
                .summary-box {{ display: flex; gap: 20px; margin: 20px 0; }}
                .stat-card {{ flex: 1; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; text-align: center; }}
                .stat-card.warning {{ background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); }}
                .stat-card.success {{ background: linear-gradient(135deg, #84fab0 0%, #8fd3f4 100%); color: #333; }}
                .stat-value {{ font-size: 36px; font-weight: bold; margin: 10px 0; }}
                .stat-label {{ font-size: 14px; opacity: 0.9; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:hover {{ background-color: #f5f5f5; }}
                .anomaly-row {{ background-color: #ffebee; }}
                .timestamp {{ color: #999; font-size: 12px; margin-top: 20px; }}
                .severity-high {{ color: #f44336; font-weight: bold; }}
                .severity-medium {{ color: #ff9800; font-weight: bold; }}
                .severity-low {{ color: #4caf50; font-weight: bold; }}
                .chart-container {{ height: 400px; margin: 20px 0; }}
                .metadata {{ background-color: #f8f9fa; padding: 15px; border-radius: 4px; margin: 20px 0; }}
                .metadata-item {{ margin: 5px 0; }}
                .footer {{ text-align: center; margin-top: 40px; color: #999; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🔍 {title}</h1>
                <p>Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
                
                <div class="summary-box">
                    <div class="stat-card">
                        <div class="stat-value">{total_records:,}</div>
                        <div class="stat-label">Total Records</div>
                    </div>
                    <div class="stat-card warning">
                        <div class="stat-value">{anomaly_count:,}</div>
                        <div class="stat-label">Anomalies Detected</div>
                    </div>
                    <div class="stat-card success">
                        <div class="stat-value">{anomaly_percentage:.1f}%</div>
                        <div class="stat-label">Anomaly Rate</div>
                    </div>
                </div>
                
                <h2>📋 Metadata</h2>
                <div class="metadata">
        """
        
        # Add metadata
        for key, value in metadata.items():
            html_content += f'<div class="metadata-item"><strong>{key}:</strong> {value}</div>\n'
        
        html_content += """
                </div>
                
                <h2>⚠️ Detected Anomalies</h2>
        """
        
        # Add anomalies table if there are any
        if anomalies:
            html_content += """
                <table>
                    <thead>
                        <tr>
                            <th>Timestamp</th>
                            <th>Farm ID</th>
                            <th>Animal Type</th>
                            <th>Severity</th>
                            <th>Score</th>
                            <th>Description</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            
            for anomaly in anomalies:
                severity_class = f"severity-{anomaly.get('severity', 'low').lower()}"
                html_content += f"""
                        <tr class="anomaly-row">
                            <td>{anomaly.get('timestamp', 'N/A')}</td>
                            <td>{anomaly.get('farm_id', 'N/A')}</td>
                            <td>{anomaly.get('animal_type', 'N/A')}</td>
                            <td class="{severity_class}">{anomaly.get('severity', 'UNKNOWN').upper()}</td>
                            <td>{anomaly.get('score', 0):.3f}</td>
                            <td>{anomaly.get('description', 'N/A')}</td>
                        </tr>
                """
            
            html_content += """
                    </tbody>
                </table>
            """
        else:
            html_content += '<p>✅ No anomalies detected in this period.</p>'
        
        # Add data sample
        html_content += """
                <h2>📊 Data Sample (First 10 Records)</h2>
                <table>
                    <thead>
                        <tr>
        """
        
        # Table headers
        sample_data = data.head(10)
        for col in sample_data.columns[:5]:  # Limit to first 5 columns
            html_content += f"<th>{col}</th>"
        
        html_content += """
                        </tr>
                    </thead>
                    <tbody>
        """
        
        # Table data
        for _, row in sample_data.iterrows():
            html_content += "<tr>"
            for col in sample_data.columns[:5]:
                html_content += f"<td>{row[col]}</td>"
            html_content += "</tr>"
        
        html_content += """
                    </tbody>
                </table>
                
                <div class="footer">
                    <p>Generated by Livestock Outbreak Detection System</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Write to file
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Report generated: {filepath}")
        return str(filepath)
    
    def generate_summary_report(self, 
                               reports: List[Dict],
                               start_date: datetime,
                               end_date: datetime) -> str:
        """
        Generate a summary report for multiple detection runs
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"summary_report_{timestamp}.html"
        filepath = self.output_dir / filename
        
        total_anomalies = sum(r.get('anomaly_count', 0) for r in reports)
        avg_score = sum(r.get('avg_score', 0) for r in reports) / len(reports) if reports else 0
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Summary Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; }}
                h1 {{ color: #333; border-bottom: 2px solid #4CAF50; }}
                .period {{ color: #666; font-size: 16px; margin: 10px 0; }}
                .stats {{ display: flex; gap: 20px; margin: 30px 0; }}
                .stat-card {{ flex: 1; padding: 20px; border-radius: 8px; background: #f8f9fa; text-align: center; }}
                .stat-value {{ font-size: 32px; font-weight: bold; color: #4CAF50; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📈 Summary Report</h1>
                <div class="period">
                    Period: {start_date.strftime("%Y-%m-%d %H:%M")} to {end_date.strftime("%Y-%m-%d %H:%M")}
                </div>
                
                <div class="stats">
                    <div class="stat-card">
                        <div class="stat-value">{len(reports)}</div>
                        <div>Detection Runs</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">{total_anomalies}</div>
                        <div>Total Anomalies</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">{avg_score:.3f}</div>
                        <div>Average Score</div>
                    </div>
                </div>
                
                <h2>Recent Reports</h2>
                <ul>
        """
        
        for report in reports[-5:]:  # Last 5 reports
            html_content += f'<li>{report.get("timestamp")} - {report.get("anomaly_count", 0)} anomalies</li>'
        
        html_content += """
                </ul>
            </div>
        </body>
        </html>
        """
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return str(filepath)


# Global instance
_report_generator: Optional[ReportGenerator] = None


def get_report_generator(output_dir: str = "outputs/reports") -> ReportGenerator:
    """Get or create the global report generator"""
    global _report_generator
    if _report_generator is None:
        _report_generator = ReportGenerator(output_dir)
    return _report_generator