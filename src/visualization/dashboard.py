"""
Visualization dashboard for livestock health monitoring
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os

class HealthDashboard:
    """Creates visualizations for livestock health monitoring"""
    
    def __init__(self, output_dir: str = './outputs/reports'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def plot_health_timeline(self, df: pd.DataFrame, 
                            animal_id: str = None,
                            metrics: List[str] = None) -> go.Figure:
        """
        Plot health metrics timeline for an animal
        
        Args:
            df: DataFrame with health metrics
            animal_id: Specific animal to plot (None for all)
            metrics: List of metrics to plot
            
        Returns:
            Plotly Figure object
        """
        if metrics is None:
            metrics = ['temperature', 'heart_rate', 'activity_level']
        
        # Filter data if animal_id is specified
        plot_df = df.copy()
        if animal_id:
            plot_df = plot_df[plot_df['tag_id'] == animal_id]
        
        if len(plot_df) == 0:
            raise ValueError("No data to plot")
        
        # Create subplots
        fig = make_subplots(
            rows=len(metrics), 
            cols=1,
            subplot_titles=[m.replace('_', ' ').title() for m in metrics],
            vertical_spacing=0.1
        )
        
        for i, metric in enumerate(metrics, 1):
            if metric not in plot_df.columns:
                continue
            
            # Create scatter plot
            scatter = go.Scatter(
                x=plot_df['date'],
                y=plot_df[metric],
                mode='lines+markers',
                name=metric.replace('_', ' ').title(),
                hovertemplate='Date: %{x}<br>Value: %{y:.2f}<extra></extra>'
            )
            
            fig.add_trace(scatter, row=i, col=1)
            
            # Highlight anomalies if available
            anomaly_col = f'{metric}_anomaly'
            if anomaly_col in plot_df.columns:
                anomalies = plot_df[plot_df[anomaly_col]]
                if not anomalies.empty:
                    anomaly_scatter = go.Scatter(
                        x=anomalies['date'],
                        y=anomalies[metric],
                        mode='markers',
                        marker=dict(
                            color='red',
                            size=10,
                            symbol='x'
                        ),
                        name=f'{metric} Anomaly',
                        hovertemplate='ANOMALY<br>Date: %{x}<br>Value: %{y:.2f}<extra></extra>'
                    )
                    fig.add_trace(anomaly_scatter, row=i, col=1)
        
        # Update layout
        fig.update_layout(
            height=300 * len(metrics),
            title_text=f"Health Metrics Timeline{' - ' + animal_id if animal_id else ''}",
            showlegend=True,
            hovermode='x unified'
        )
        
        return fig
    
    def plot_outbreak_clusters(self, clusters: List[Dict], 
                              df: pd.DataFrame) -> go.Figure:
        """
        Visualize outbreak clusters
        
        Args:
            clusters: List of cluster dictionaries
            df: Original dataframe for context
            
        Returns:
            Plotly Figure object
        """
        if not clusters:
            # Create empty figure
            fig = go.Figure()
            fig.add_annotation(
                text="No outbreak clusters detected",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=20)
            )
            return fig
        
        # Create timeline of clusters
        fig = go.Figure()
        
        for i, cluster in enumerate(clusters):
            # Add cluster as a rectangle
            fig.add_shape(
                type="rect",
                x0=cluster['start_date'],
                x1=cluster['end_date'],
                y0=i - 0.4,
                y1=i + 0.4,
                fillcolor="red" if cluster.get('severity') == 'critical' else "orange",
                opacity=0.5,
                line_width=0
            )
            
            # Add annotation
            fig.add_annotation(
                x=cluster['start_date'],
                y=i,
                text=f"Farm {cluster['farm_id']}: {cluster['affected_animals']} animals",
                showarrow=False,
                yshift=10
            )
        
        # Update layout
        fig.update_layout(
            title="Outbreak Clusters Timeline",
            xaxis_title="Date",
            yaxis_title="Cluster",
            height=400,
            showlegend=False
        )
        
        # Set y-axis ticks
        fig.update_yaxes(
            tickvals=list(range(len(clusters))),
            ticktext=[f"Cluster {i+1}" for i in range(len(clusters))]
        )
        
        return fig
    
    def create_summary_report(self, df: pd.DataFrame, 
                             clusters: List[Dict]) -> str:
        """
        Create HTML summary report
        
        Args:
            df: DataFrame with health metrics
            clusters: List of outbreak clusters
            
        Returns:
            HTML string
        """
        # Basic statistics
        total_animals = df['tag_id'].nunique()
        total_records = len(df)
        anomaly_count = df['is_anomaly'].sum() if 'is_anomaly' in df.columns else 0
        
        # Animal type distribution
        if 'animal_type' in df.columns:
            animal_dist = df['animal_type'].value_counts().to_dict()
        else:
            animal_dist = {}
        
        # Create HTML
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Livestock Health Report - {datetime.now().strftime('%Y-%m-%d')}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
                .stat-box {{ display: inline-block; margin: 10px; padding: 15px; 
                            border: 1px solid #ddd; border-radius: 5px; }}
                .critical {{ color: #dc3545; font-weight: bold; }}
                .warning {{ color: #ffc107; font-weight: bold; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Livestock Health Monitoring Report</h1>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            
            <h2>Summary Statistics</h2>
            <div>
                <div class="stat-box">
                    <h3>Animals Monitored</h3>
                    <p>{total_animals}</p>
                </div>
                <div class="stat-box">
                    <h3>Health Records</h3>
                    <p>{total_records}</p>
                </div>
                <div class="stat-box">
                    <h3>Anomalies Detected</h3>
                    <p class="{ 'critical' if anomaly_count > 0 else '' }">{anomaly_count}</p>
                </div>
                <div class="stat-box">
                    <h3>Outbreak Clusters</h3>
                    <p class="{ 'critical' if len(clusters) > 0 else '' }">{len(clusters)}</p>
                </div>
            </div>
            
            <h2>Animal Distribution</h2>
            <table>
                <tr>
                    <th>Animal Type</th>
                    <th>Count</th>
                    <th>Percentage</th>
                </tr>
        """
        
        for animal_type, count in animal_dist.items():
            percentage = (count / total_animals * 100) if total_animals > 0 else 0
            html += f"""
                <tr>
                    <td>{animal_type.title()}</td>
                    <td>{count}</td>
                    <td>{percentage:.1f}%</td>
                </tr>
            """
        
        html += """
            </table>
            
            <h2>Outbreak Clusters</h2>
        """
        
        if clusters:
            html += """
            <table>
                <tr>
                    <th>Farm ID</th>
                    <th>Start Date</th>
                    <th>Affected Animals</th>
                    <th>Severity</th>
                    <th>Metrics Affected</th>
                </tr>
            """
            
            for cluster in clusters:
                severity = cluster.get('severity', 'medium')
                html += f"""
                <tr>
                    <td>{cluster.get('farm_id', 'Unknown')}</td>
                    <td>{cluster.get('start_date', 'Unknown')}</td>
                    <td>{cluster.get('affected_animals', 0)}</td>
                    <td class="{severity}">{severity.upper()}</td>
                    <td>{', '.join(cluster.get('metrics_affected', []))}</td>
                </tr>
                """
            
            html += "</table>"
        else:
            html += "<p>No outbreak clusters detected during this period.</p>"
        
        html += """
            <hr>
            <footer>
                <p><em>Generated by Livestock Outbreak Detection System v1.0</em></p>
            </footer>
        </body>
        </html>
        """
        
        return html
    
    def save_report(self, html_content: str, filename: str = None):
        """
        Save HTML report to file
        
        Args:
            html_content: HTML string
            filename: Output filename
        """
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"health_report_{timestamp}.html"
        
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return filepath