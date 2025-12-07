#!/usr/bin/env python3
"""
Simple Flask dashboard for visualizing detection results
"""

from flask import Flask, render_template, jsonify, request
import pandas as pd
import sqlite3
import os
from datetime import datetime, timedelta
import json
import yaml 

import json
from src.data_quality.analyzer import DataQualityAnalyzer

app = Flask(__name__)

def get_db_connection():
    conn = sqlite3.connect('data/livestock.db')
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/health_metrics')
def get_health_metrics():
    conn = get_db_connection()
    
    # Get latest metrics
    query = """
    SELECT tag_id, date, temperature, heart_rate, activity_level, 
           is_anomaly, anomaly_score
    FROM health_metrics 
    WHERE date >= date('now', '-30 days')
    ORDER BY date DESC
    LIMIT 1000
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Convert to list of records
    records = df.to_dict('records')
    
    return jsonify({
        'success': True,
        'count': len(records),
        'data': records
    })

@app.route('/api/alerts')
def get_alerts():
    conn = get_db_connection()
    
    query = """
    SELECT * FROM outbreak_alerts 
    WHERE is_resolved = 0
    ORDER BY created_at DESC
    LIMIT 50
    """
    
    alerts = conn.execute(query).fetchall()
    conn.close()
    
    # Convert to dict
    alerts_list = [dict(alert) for alert in alerts]
    
    return jsonify({
        'success': True,
        'count': len(alerts_list),
        'alerts': alerts_list
    })

@app.route('/api/summary')
def get_summary():
    conn = get_db_connection()
    
    # Get summary statistics
    summary_query = """
    SELECT 
        COUNT(DISTINCT tag_id) as total_animals,
        COUNT(*) as total_records,
        SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) as anomaly_count,
        COUNT(DISTINCT farm_id) as farms_monitored
    FROM health_metrics
    WHERE date >= date('now', '-30 days')
    """
    
    summary = conn.execute(summary_query).fetchone()
    
    # Get recent alerts count
    alerts_query = """
    SELECT 
        COUNT(*) as active_alerts,
        COUNT(CASE WHEN severity = 'critical' THEN 1 END) as critical_alerts
    FROM outbreak_alerts
    WHERE is_resolved = 0
    """
    
    alerts_summary = conn.execute(alerts_query).fetchone()
    
    conn.close()
    
    return jsonify({
        'success': True,
        'summary': dict(summary),
        'alerts': dict(alerts_summary)
    })

@app.route('/api/anomaly_timeline')
def get_anomaly_timeline():
    conn = get_db_connection()
    
    query = """
    SELECT 
        date(date) as day,
        COUNT(*) as total_records,
        SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) as anomalies,
        AVG(anomaly_score) as avg_score
    FROM health_metrics
    WHERE date >= date('now', '-30 days')
    GROUP BY date(date)
    ORDER BY day
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    timeline = df.to_dict('records')
    
    return jsonify({
        'success': True,
        'timeline': timeline
    })

    import yaml  # Add at top with other imports

# Add after existing routes in dashboard.py

@app.route('/api/export/anomalies')
def export_anomalies():
    """Export anomaly data"""
    try:
        conn = get_db_connection()
        
        # Get anomalies from database
        query = """
        SELECT * FROM health_metrics 
        WHERE is_anomaly = 1
        ORDER BY date DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            return jsonify({
                'success': False,
                'error': 'No anomaly data to export'
            })
        
        # Export data
        from src.export.exporter import DataExporter
        exporter = DataExporter()
        
        exported_files = exporter.export_anomalies(df)
        
        return jsonify({
            'success': True,
            'message': f'Exported {len(df)} anomalies',
            'files': exported_files
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/export/alerts')
def export_alerts():
    """Export alert data"""
    try:
        conn = get_db_connection()
        
        # Get alerts from database
        query = """
        SELECT * FROM outbreak_alerts 
        ORDER BY created_at DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            return jsonify({
                'success': False,
                'error': 'No alert data to export'
            })
        
        # Export data
        from src.export.exporter import DataExporter
        exporter = DataExporter()
        
        exported_files = exporter.export_alerts(df)
        
        return jsonify({
            'success': True,
            'message': f'Exported {len(df)} alerts',
            'files': exported_files
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/export/health_metrics')
def export_health_metrics():
    """Export health metrics data"""
    try:
        days = request.args.get('days', 30, type=int)
        
        conn = get_db_connection()
        
        # Get health metrics
        query = f"""
        SELECT * FROM health_metrics 
        WHERE date >= date('now', '-{days} days')
        ORDER BY date DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            return jsonify({
                'success': False,
                'error': 'No health metrics data to export'
            })
        
        # Export data
        from src.export.exporter import DataExporter
        exporter = DataExporter()
        
        exported_files = exporter.export_health_metrics(df, f'last_{days}_days')
        
        return jsonify({
            'success': True,
            'message': f'Exported {len(df)} health records',
            'files': exported_files
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/export/summary_report')
def export_summary_report():
    """Generate and export summary report"""
    try:
        format_type = request.args.get('format', 'txt')
        
        if format_type not in ['txt', 'md']:
            return jsonify({
                'success': False,
                'error': 'Invalid format. Use "txt" or "md"'
            })
        
        conn = get_db_connection()
        
        # Get anomalies
        anomalies_query = """
        SELECT * FROM health_metrics 
        WHERE date >= date('now', '-7 days')
        """
        anomalies_df = pd.read_sql_query(anomalies_query, conn)
        
        # Get alerts
        alerts_query = """
        SELECT * FROM outbreak_alerts 
        WHERE created_at >= date('now', '-7 days')
        ORDER BY created_at DESC
        """
        alerts_df = pd.read_sql_query(alerts_query, conn)
        conn.close()
        
        # Convert alerts to list of dicts
        alerts = alerts_df.to_dict('records')
        
        # Generate report
        from src.export.exporter import DataExporter
        exporter = DataExporter()
        
        report_file = exporter.generate_summary_report(
            anomalies_df, 
            alerts, 
            output_format=format_type
        )
        
        return jsonify({
            'success': True,
            'message': 'Summary report generated',
            'file': report_file
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/exports/list')
def list_exports():
    """List available export files"""
    try:
        days = request.args.get('days', 7, type=int)
        
        from src.export.exporter import DataExporter
        exporter = DataExporter()
        
        exports = exporter.list_exports(days=days)
        
        return jsonify({
            'success': True,
            'count': len(exports),
            'exports': exports
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/exports/cleanup', methods=['POST'])
def cleanup_exports():
    """Cleanup old export files"""
    try:
        days_to_keep = request.json.get('days_to_keep', 30)
        
        from src.export.exporter import DataExporter
        exporter = DataExporter()
        
        deleted_count = exporter.cleanup_old_exports(days_to_keep)
        
        return jsonify({
            'success': True,
            'message': f'Deleted {deleted_count} old export files'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/exports/download/<filename>')
def download_export(filename):
    """Download an exported file"""
    try:
        # Security check: prevent directory traversal
        if '..' in filename or filename.startswith('/'):
            return "Invalid filename", 400
        
        export_dir = './outputs/exports'
        filepath = os.path.join(export_dir, filename)
        
        if not os.path.exists(filepath):
            return "File not found", 404
        
        # Determine content type
        ext = os.path.splitext(filename)[1].lower()
        content_types = {
            '.csv': 'text/csv',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.json': 'application/json',
            '.txt': 'text/plain',
            '.md': 'text/markdown'
        }
        
        content_type = content_types.get(ext, 'application/octet-stream')
        
        return send_file(
            filepath,
            as_attachment=True,
            download_name=filename,
            mimetype=content_type
        )
        
    except Exception as e:
        return str(e), 500
# Add these imports at the top if not already present
import json
from src.data_quality.analyzer import DataQualityAnalyzer

# Add after existing routes in dashboard.py

@app.route('/api/quality/analyze')
def analyze_data_quality():
    """Analyze data quality"""
    try:
        days = request.args.get('days', 30, type=int)
        
        conn = get_db_connection()
        
        # Get data for analysis
        query = f"""
        SELECT * FROM health_metrics 
        WHERE date >= date('now', '-{days} days')
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Analyze data quality
        analyzer = DataQualityAnalyzer()
        analysis = analyzer.analyze_dataframe(df)
        
        return jsonify({
            'success': True,
            'analysis': analysis
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/quality/report')
def get_quality_report():
    """Get data quality report"""
    try:
        format_type = request.args.get('format', 'text')
        
        if format_type not in ['text', 'html', 'json']:
            return jsonify({
                'success': False,
                'error': 'Invalid format. Use "text", "html", or "json"'
            })
        
        days = request.args.get('days', 30, type=int)
        
        conn = get_db_connection()
        
        # Get data for analysis
        query = f"""
        SELECT * FROM health_metrics 
        WHERE date >= date('now', '-{days} days')
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Analyze data quality
        analyzer = DataQualityAnalyzer()
        analysis = analyzer.analyze_dataframe(df)
        
        if format_type == 'json':
            return jsonify({
                'success': True,
                'analysis': analysis
            })
        else:
            report = analyzer.generate_quality_report(analysis, format_type)
            
            # Save report to file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_dir = 'outputs/quality_reports'
            os.makedirs(report_dir, exist_ok=True)
            
            if format_type == 'text':
                filepath = os.path.join(report_dir, f'quality_report_{timestamp}.txt')
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(report)
            else:  # html
                filepath = os.path.join(report_dir, f'quality_report_{timestamp}.html')
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(report)
            
            return jsonify({
                'success': True,
                'report': report,
                'filepath': filepath,
                'format': format_type
            })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/quality/issues')
def get_quality_issues():
    """Get data quality issues"""
    try:
        severity = request.args.get('severity', None)
        
        conn = get_db_connection()
        
        # Get recent data
        query = """
        SELECT * FROM health_metrics 
        WHERE date >= date('now', '-30 days')
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Analyze data quality
        analyzer = DataQualityAnalyzer()
        analysis = analyzer.analyze_dataframe(df)
        
        issues = analysis.get('issues', [])
        
        # Filter by severity if specified
        if severity:
            issues = [issue for issue in issues if issue.get('severity') == severity.lower()]
        
        return jsonify({
            'success': True,
            'count': len(issues),
            'issues': issues
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/quality/score')
def get_quality_score():
    """Get overall data quality score"""
    try:
        conn = get_db_connection()
        
        # Get recent data
        query = """
        SELECT * FROM health_metrics 
        WHERE date >= date('now', '-30 days')
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Analyze data quality
        analyzer = DataQualityAnalyzer()
        analysis = analyzer.analyze_dataframe(df)
        
        score = analysis.get('quality_score', 0)
        
        return jsonify({
            'success': True,
            'score': score,
            'rating': _get_quality_rating(score)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

def _get_quality_rating(score):
    """Convert numeric score to rating"""
    if score >= 90:
        return {'text': 'Excellent', 'color': '#28a745', 'level': 5}
    elif score >= 70:
        return {'text': 'Good', 'color': '#ffc107', 'level': 4}
    elif score >= 50:
        return {'text': 'Fair', 'color': '#fd7e14', 'level': 3}
    elif score >= 30:
        return {'text': 'Poor', 'color': '#dc3545', 'level': 2}
    else:
        return {'text': 'Very Poor', 'color': '#6c757d', 'level': 1}

@app.route('/api/quality/metrics')
def get_quality_metrics():
    """Get detailed quality metrics"""
    try:
        conn = get_db_connection()
        
        # Get data for last 90 days
        query = """
        SELECT * FROM health_metrics 
        WHERE date >= date('now', '-90 days')
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            return jsonify({
                'success': True,
                'metrics': {},
                'message': 'No data available for analysis'
            })
        
        # Calculate various metrics
        metrics = {}
        
        # Completeness metrics
        total_cells = len(df) * len(df.columns)
        non_missing_cells = df.notna().sum().sum()
        metrics['completeness'] = {
            'overall': (non_missing_cells / total_cells * 100) if total_cells > 0 else 0,
            'by_column': {}
        }
        
        for column in df.columns:
            missing_pct = (df[column].isna().sum() / len(df)) * 100
            metrics['completeness']['by_column'][column] = {
                'missing_percent': float(missing_pct),
                'missing_count': int(df[column].isna().sum())
            }
        
        # Validity metrics (for numeric columns)
        numeric_cols = ['temperature', 'heart_rate', '
if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    # Create a simple HTML template
    with open('templates/index.html', 'w') as f:
        f.write("""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Livestock Outbreak Detection</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .dashboard { display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; }
                .card { border: 1px solid #ddd; padding: 15px; border-radius: 5px; }
                .critical { color: #dc3545; font-weight: bold; }
                .warning { color: #ffc107; font-weight: bold; }
                table { width: 100%; border-collapse: collapse; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; }
            </style>
        </head>
        <body>
            <h1>Livestock Disease Outbreak Detection Dashboard</h1>
            <p><em>Offline MVP - Last updated: <span id="lastUpdate"></span></em></p>
            
            <div class="dashboard">
                <div class="card">
                    <h3>System Summary</h3>
                    <div id="summary"></div>
                </div>
                
                <div class="card">
                    <h3>Active Alerts</h3>
                    <div id="alerts"></div>
                </div>
                
                <div class="card">
                    <h3>Anomaly Timeline</h3>
                    <div id="timeline"></div>
                </div>
            </div>
            
            <div class="card" style="grid-column: span 3; margin-top: 20px;">
                <h3>Recent Health Metrics</h3>
                <table id="metricsTable">
                    <thead>
                        <tr>
                            <th>Animal ID</th>
                            <th>Date</th>
                            <th>Temperature</th>
                            <th>Heart Rate</th>
                            <th>Activity</th>
                            <th>Anomaly Score</th>
                        </tr>
                    </thead>
                    <tbody id="metricsBody">
                    </tbody>
                </table>
            </div>
            
            <script>
                async function loadData() {
                    try {
                        // Load summary
                        const summaryRes = await fetch('/api/summary');
                        const summaryData = await summaryRes.json();
                        
                        if (summaryData.success) {
                            const s = summaryData.summary;
                            document.getElementById('summary').innerHTML = `
                                <p>Animals Monitored: ${s.total_animals}</p>
                                <p>Farms: ${s.farms_monitored}</p>
                                <p>Anomalies Detected: ${s.anomaly_count}</p>
                                <p>Active Alerts: ${summaryData.alerts.active_alerts}</p>
                                <p class="${summaryData.alerts.critical_alerts > 0 ? 'critical' : ''}">
                                    Critical Alerts: ${summaryData.alerts.critical_alerts}
                                </p>
                            `;
                        }
                        
                        // Load alerts
                        const alertsRes = await fetch('/api/alerts');
                        const alertsData = await alertsRes.json();
                        
                        if (alertsData.success) {
                            const alertsHtml = alertsData.alerts.map(alert => `
                                <div class="${alert.severity}">
                                    ${alert.severity.toUpperCase()}: ${alert.description}
                                    <br><small>${new Date(alert.created_at).toLocaleString()}</small>
                                </div>
                            `).join('');
                            document.getElementById('alerts').innerHTML = alertsHtml || '<p>No active alerts</p>';
                        }
                        
                        // Load timeline
                        const timelineRes = await fetch('/api/anomaly_timeline');
                        const timelineData = await timelineRes.json();
                        
                        if (timelineData.success) {
                            const timelineHtml = timelineData.timeline.slice(-10).map(day => `
                                <div>
                                    ${new Date(day.day).toLocaleDateString()}: 
                                    ${day.anomalies} anomalies (avg score: ${parseFloat(day.avg_score).toFixed(2)})
                                </div>
                            `).join('');
                            document.getElementById('timeline').innerHTML = timelineHtml;
                        }
                        
                        // Load recent metrics
                        const metricsRes = await fetch('/api/health_metrics');
                        const metricsData = await metricsRes.json();
                        
                        if (metricsData.success) {
                            const metricsHtml = metricsData.data.slice(0, 20).map(metric => `
                                <tr class="${metric.is_anomaly ? 'warning' : ''}">
                                    <td>${metric.tag_id}</td>
                                    <td>${new Date(metric.date).toLocaleDateString()}</td>
                                    <td>${parseFloat(metric.temperature).toFixed(1)}°C</td>
                                    <td>${parseFloat(metric.heart_rate).toFixed(0)} BPM</td>
                                    <td>${parseFloat(metric.activity_level).toFixed(2)}</td>
                                    <td>${parseFloat(metric.anomaly_score).toFixed(2)}</td>
                                </tr>
                            `).join('');
                            document.getElementById('metricsBody').innerHTML = metricsHtml;
                        }
                        
                        // Update timestamp
                        document.getElementById('lastUpdate').textContent = new Date().toLocaleString();
                        
                    } catch (error) {
                        console.error('Error loading data:', error);
                    }
                }
                
                // Load data on page load
                document.addEventListener('DOMContentLoaded', loadData);
                
                // Refresh every 5 minutes
                setInterval(loadData, 300000);
            </script>
        </body>
        </html>
        """)
    
    print("Starting dashboard server on http://localhost:5000")
    print("Press Ctrl+C to stop")
    app.run(debug=True, host='0.0.0.0', port=5000)