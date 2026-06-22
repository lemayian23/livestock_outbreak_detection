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
        timestamp = date.now().strftime("%Y%m%d_%H%M%S")
        filename = f"anomaly_report_{timestamp}.html"
        filepath = self.output_dir / filename

        # Basic statistics
        total_records = len(data)
        anomaly_count = len(anomalies)
        anomaly_percentage = (anomaly_count / total_records * 100) if total_records > 0 else 0

        # Create HTML content
        hml_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{title}</title>