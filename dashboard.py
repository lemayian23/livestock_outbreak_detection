#!/usr/bin/env python3
"""
Simple Flask dashboard for visualizing detection results
"""

from flask import Flask, render_template, jsonify, request, send_file
import pandas as pd
import sqlite3
import os
from datetime import datetime, timedelta
import json
import yaml 
from src.logging.alert_logger import AlertLogger
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

# Data Quality Routes
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
        
        # Get rating
        if score >= 90:
            rating = {'text': 'Excellent', 'color': '#28a745', 'level': 5}
        elif score >= 70:
            rating = {'text': 'Good', 'color': '#ffc107', 'level': 4}
        elif score >= 50:
            rating = {'text': 'Fair', 'color': '#fd7e14', 'level': 3}
        elif score >= 30:
            rating = {'text': 'Poor', 'color': '#dc3545', 'level': 2}
        else:
            rating = {'text': 'Very Poor', 'color': '#6c757d', 'level': 1}
        
        return jsonify({
            'success': True,
            'score': score,
            'rating': rating
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

# Export Routes
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

# Alert Log Routes
@app.route('/api/logs/alerts')
def get_alert_logs():
    """Get recent alert logs"""
    try:
        days = request.args.get('days', 7, type=int)
        
        logger = AlertLogger()
        alerts = logger.get_recent_alerts(days)
        
        return jsonify({
            'success': True,
            'count': len(alerts),
            'alerts': alerts
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/logs/search')
def search_alert_logs():
    """Search alert logs"""
    try:
        keyword = request.args.get('keyword', '')
        severity = request.args.get('severity', '')
        farm_id = request.args.get('farm_id', '')
        days = request.args.get('days', 30, type=int)
        
        logger = AlertLogger()
        results = logger.search_alerts(keyword, severity, farm_id, days)
        
        return jsonify({
            'success': True,
            'count': len(results),
            'results': results
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/logs/stats')
def get_alert_stats():
    """Get alert statistics"""
    try:
        days = request.args.get('days', 7, type=int)
        
        logger = AlertLogger()
        stats = logger.get_alert_stats(days)
        
        return jsonify({
            'success': True,
            'stats': stats
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/logs/export')
def export_alert_logs():
    """Export alert logs"""
    try:
        days = request.args.get('days', 30, type=int)
        format_type = request.args.get('format', 'csv')
        
        if format_type not in ['csv', 'json']:
            return jsonify({
                'success': False,
                'error': 'Invalid format. Use "csv" or "json"'
            })
        
        logger = AlertLogger()
        filepath = logger.export_alerts(days, format_type)
        
        if filepath and os.path.exists(filepath):
            filename = os.path.basename(filepath)
            return jsonify({
                'success': True,
                'message': f'Exported {days} days of alerts',
                'filepath': filepath,
                'filename': filename
            })
        else:
            return jsonify({
                'success': False,
                'error': 'No alerts to export or export failed'
            })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/logs/summary')
def get_log_summary():
    """Get log system summary"""
    try:
        logger = AlertLogger()
        summary = logger.get_log_summary()
        
        return jsonify({
            'success': True,
            'summary': summary
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/logs/cleanup', methods=['POST'])
def cleanup_logs():
    """Cleanup old log files"""
    try:
        days_to_keep = request.json.get('days_to_keep', 90)
        
        logger = AlertLogger()
        deleted = logger.cleanup_old_logs(days_to_keep)
        
        return jsonify({
            'success': True,
            'message': f'Deleted {deleted} old log files'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    # Create a complete HTML template with all features
    html_template = """
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
            
            /* Export buttons */
            .export-btn {
                padding: 10px 15px;
                background-color: #4CAF50;
                color: white;
                border: none;
                border-radius: 5px;
                cursor: pointer;
                font-size: 14px;
                transition: background-color 0.3s;
            }
            
            .export-btn:hover {
                background-color: #45a049;
            }
            
            .file-link {
                display: inline-block;
                margin: 5px;
                padding: 8px 12px;
                background-color: #e7f3fe;
                border: 1px solid #b3d9ff;
                border-radius: 4px;
                text-decoration: none;
                color: #0066cc;
            }
            
            .file-link:hover {
                background-color: #d1e7ff;
            }
            
            .export-item {
                padding: 8px;
                border-bottom: 1px solid #eee;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }
            
            .export-item:last-child {
                border-bottom: none;
            }
            
            /* Quality buttons */
            .quality-btn {
                padding: 10px 15px;
                background-color: #17a2b8;
                color: white;
                border: none;
                border-radius: 5px;
                cursor: pointer;
                font-size: 14px;
                transition: background-color 0.3s;
            }
            
            .quality-btn:hover {
                background-color: #138496;
            }
            
            .quality-score {
                text-align: center;
                padding: 20px;
                margin: 15px 0;
                border-radius: 10px;
                font-size: 18px;
            }
            
            .quality-excellent { background-color: #d4edda; border-left: 5px solid #28a745; }
            .quality-good { background-color: #fff3cd; border-left: 5px solid #ffc107; }
            .quality-fair { background-color: #ffe5d0; border-left: 5px solid #fd7e14; }
            .quality-poor { background-color: #f8d7da; border-left: 5px solid #dc3545; }
            .quality-very-poor { background-color: #e2e3e5; border-left: 5px solid #6c757d; }
            
            .dimension-card {
                border: 1px solid #dee2e6;
                border-radius: 5px;
                padding: 15px;
                margin: 10px 0;
                background-color: #f8f9fa;
            }
            
            .progress-container {
                height: 20px;
                background-color: #e9ecef;
                border-radius: 10px;
                margin: 10px 0;
                overflow: hidden;
            }
            
            .progress-bar {
                height: 100%;
                border-radius: 10px;
                transition: width 0.5s;
            }
            
            .issue-card {
                padding: 10px;
                margin: 5px 0;
                border-radius: 5px;
                border-left: 4px solid;
            }
            
            .issue-high { background-color: #f8d7da; border-left-color: #dc3545; }
            .issue-medium { background-color: #fff3cd; border-left-color: #ffc107; }
            .issue-low { background-color: #d4edda; border-left-color: #28a745; }
            
            /* Log buttons */
            .log-btn {
                padding: 8px 12px;
                background-color: #6c757d;
                color: white;
                border: none;
                border-radius: 5px;
                cursor: pointer;
                font-size: 14px;
            }
            
            .log-btn:hover {
                background-color: #5a6268;
            }
            
            .log-entry {
                padding: 8px;
                margin: 5px 0;
                border-left: 4px solid;
                background-color: #f8f9fa;
                border-radius: 3px;
            }
            
            .log-critical { border-left-color: #dc3545; }
            .log-high { border-left-color: #fd7e14; }
            .log-medium { border-left-color: #ffc107; }
            .log-low { border-left-color: #28a745; }
            .log-info { border-left-color: #17a2b8; }
            
            .stat-card {
                display: inline-block;
                margin: 0 10px 10px 0;
                padding: 10px;
                background-color: white;
                border: 1px solid #dee2e6;
                border-radius: 5px;
                min-width: 100px;
                text-align: center;
            }
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
        
        <!-- Data Export Section -->
        <div class="card" style="grid-column: span 3; margin-top: 20px;">
            <h3>📤 Data Export</h3>
            <div style="display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 15px;">
                <button onclick="exportData('anomalies')" class="export-btn">
                    📊 Export Anomalies
                </button>
                <button onclick="exportData('alerts')" class="export-btn">
                    ⚠️ Export Alerts
                </button>
                <button onclick="exportData('health_metrics')" class="export-btn">
                    📈 Export Health Metrics
                </button>
                <button onclick="exportData('summary_txt')" class="export-btn">
                    📄 Summary Report (TXT)
                </button>
                <button onclick="exportData('summary_md')" class="export-btn">
                    📋 Summary Report (Markdown)
                </button>
            </div>
            
            <div id="exportStatus" style="display: none; padding: 10px; background-color: #f0f8ff; border-radius: 5px; margin-top: 10px;">
                <p id="exportMessage"></p>
                <div id="exportLinks" style="margin-top: 10px;"></div>
            </div>
            
            <div style="margin-top: 20px;">
                <h4>Recent Exports</h4>
                <button onclick="loadExports()" style="margin-bottom: 10px;">🔄 Refresh List</button>
                <div id="exportsList" style="max-height: 200px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; border-radius: 5px;">
                    <p>Click "Refresh List" to view recent exports</p>
                </div>
            </div>
        </div>
        
        <!-- Data Quality Dashboard -->
        <div class="card" style="grid-column: span 3; margin-top: 20px;">
            <h3>📊 Data Quality Dashboard</h3>
            
            <div style="display: flex; gap: 10px; margin-bottom: 15px;">
                <button onclick="analyzeDataQuality()" class="quality-btn">
                    🔍 Analyze Data Quality
                </button>
                <button onclick="getQualityReport('text')" class="quality-btn">
                    📄 Text Report
                </button>
                <button onclick="getQualityReport('html')" class="quality-btn">
                    🌐 HTML Report
                </button>
                <button onclick="getQualityIssues()" class="quality-btn">
                    ⚠️ View Issues
                </button>
            </div>
            
            <div id="qualityDashboard" style="display: none;">
                <!-- Quality score will be inserted here -->
            </div>
            
            <div id="qualityStatus" style="margin-top: 15px; padding: 10px; border-radius: 5px; display: none;">
                <!-- Status messages will appear here -->
            </div>
            
            <div id="qualityDetails" style="margin-top: 20px;">
                <!-- Detailed metrics will appear here -->
            </div>
        </div>
        
        <!-- Alert Logs Section -->
        <div class="card" style="grid-column: span 3; margin-top: 20px;">
            <h3>📋 Alert Logs</h3>
            
            <div style="display: flex; gap: 10px; margin-bottom: 15px;">
                <button onclick="loadAlertLogs()" class="log-btn">
                    🔄 Refresh Logs
                </button>
                <button onclick="searchLogs()" class="log-btn">
                    🔍 Search Logs
                </button>
                <button onclick="exportLogs('csv')" class="log-btn">
                    📥 Export CSV
                </button>
                <button onclick="exportLogs('json')" class="log-btn">
                    📥 Export JSON
                </button>
            </div>
            
            <div style="margin-bottom: 15px; display: flex; gap: 10px;">
                <input type="text" id="searchKeyword" placeholder="Search keyword" style="padding: 8px; flex: 1;">
                <select id="searchSeverity" style="padding: 8px;">
                    <option value="">All Severities</option>
                    <option value="critical">Critical</option>
                    <option value="high">High</option>
                    <option value="medium">Medium</option>
                    <option value="low">Low</option>
                </select>
            </div>
            
            <div id="logStats" style="background-color: #f8f9fa; padding: 10px; border-radius: 5px; margin-bottom: 15px;">
                <!-- Stats will appear here -->
            </div>
            
            <div id="alertLogs" style="max-height: 300px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; border-radius: 5px;">
                <p>Click "Refresh Logs" to load alert history</p>
            </div>
            
            <div id="logStatus" style="margin-top: 10px; padding: 10px; display: none;"></div>
        </div>
        
        <script>
            // Main dashboard functions
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
            
            // Export functions
            async function exportData(type) {
                const exportStatus = document.getElementById('exportStatus');
                const exportMessage = document.getElementById('exportMessage');
                const exportLinks = document.getElementById('exportLinks');
                
                exportStatus.style.display = 'block';
                exportMessage.textContent = 'Exporting data...';
                exportLinks.innerHTML = '';
                
                try {
                    let url;
                    let params = {};
                    
                    switch(type) {
                        case 'anomalies':
                            url = '/api/export/anomalies';
                            break;
                        case 'alerts':
                            url = '/api/export/alerts';
                            break;
                        case 'health_metrics':
                            url = '/api/export/health_metrics';
                            params = { days: 30 };
                            break;
                        case 'summary_txt':
                            url = '/api/export/summary_report';
                            params = { format: 'txt' };
                            break;
                        case 'summary_md':
                            url = '/api/export/summary_report';
                            params = { format: 'md' };
                            break;
                        default:
                            throw new Error('Invalid export type');
                    }
                    
                    // Add query parameters
                    if (Object.keys(params).length > 0) {
                        const queryString = new URLSearchParams(params).toString();
                        url += '?' + queryString;
                    }
                    
                    const response = await fetch(url);
                    const data = await response.json();
                    
                    if (data.success) {
                        exportMessage.textContent = data.message;
                        
                        // Show download links
                        if (data.files) {
                            exportLinks.innerHTML = '<strong>Download:</strong><br>';
                            for (const [format, filepath] of Object.entries(data.files)) {
                                const filename = filepath.split('/').pop();
                                exportLinks.innerHTML += `
                                    <a href="/api/exports/download/${filename}" class="file-link">
                                        ${format.toUpperCase()}
                                    </a>
                                `;
                            }
                        } else if (data.file) {
                            const filename = data.file.split('/').pop();
                            exportLinks.innerHTML = `
                                <strong>Download:</strong><br>
                                <a href="/api/exports/download/${filename}" class="file-link">
                                    ${filename}
                                </a>
                            `;
                        }
                        
                        // Refresh exports list
                        loadExports();
                        
                    } else {
                        exportMessage.textContent = 'Error: ' + (data.error || 'Export failed');
                        exportMessage.style.color = '#dc3545';
                    }
                    
                } catch (error) {
                    exportMessage.textContent = 'Error: ' + error.message;
                    exportMessage.style.color = '#dc3545';
                }
            }
            
            async function loadExports() {
                const exportsList = document.getElementById('exportsList');
                
                try {
                    const response = await fetch('/api/exports/list?days=7');
                    const data = await response.json();
                    
                    if (data.success && data.exports.length > 0) {
                        let html = '';
                        
                        data.exports.forEach(exp => {
                            const fileSize = exp.size_human || Math.round(exp.size / 1024) + ' KB';
                            const modified = new Date(exp.modified).toLocaleString();
                            
                            html += `
                                <div class="export-item">
                                    <div>
                                        <strong>${exp.filename}</strong><br>
                                        <small>Size: ${fileSize} | Modified: ${modified}</small>
                                    </div>
                                    <a href="/api/exports/download/${exp.filename}" class="file-link">
                                        Download
                                    </a>
                                </div>
                            `;
                        });
                        
                        exportsList.innerHTML = html;
                    } else {
                        exportsList.innerHTML = '<p>No recent exports found.</p>';
                    }
                    
                } catch (error) {
                    exportsList.innerHTML = '<p>Error loading exports: ' + error.message + '</p>';
                }
            }
            
            // Data Quality functions
            async function analyzeDataQuality() {
                const status = document.getElementById('qualityStatus');
                const dashboard = document.getElementById('qualityDashboard');
                
                status.style.display = 'block';
                status.innerHTML = '<p>Analyzing data quality...</p>';
                status.style.backgroundColor = '#e7f3fe';
                
                try {
                    const response = await fetch('/api/quality/analyze');
                    const data = await response.json();
                    
                    if (data.success) {
                        status.innerHTML = '<p>✓ Analysis complete!</p>';
                        status.style.backgroundColor = '#d4edda';
                        
                        // Display quality dashboard
                        displayQualityDashboard(data.analysis);
                        dashboard.style.display = 'block';
                        
                        // Also show detailed metrics
                        displayQualityMetrics(data.analysis);
                    } else {
                        status.innerHTML = `<p>✗ Error: ${data.error}</p>`;
                        status.style.backgroundColor = '#f8d7da';
                    }
                } catch (error) {
                    status.innerHTML = `<p>✗ Error: ${error.message}</p>`;
                    status.style.backgroundColor = '#f8d7da';
                }
            }
            
            async function getQualityReport(format) {
                const status = document.getElementById('qualityStatus');
                
                status.style.display = 'block';
                status.innerHTML = `<p>Generating ${format.toUpperCase()} report...</p>`;
                status.style.backgroundColor = '#e7f3fe';
                
                try {
                    const response = await fetch(`/api/quality/report?format=${format}`);
                    const data = await response.json();
                    
                    if (data.success) {
                        if (format === 'html') {
                            // Open HTML in new window
                            const newWindow = window.open();
                            newWindow.document.write(data.report);
                            newWindow.document.close();
                            status.innerHTML = '<p>✓ HTML report opened in new window</p>';
                        } else if (format === 'text') {
                            // Show text report in dialog
                            showTextReport(data.report);
                            status.innerHTML = '<p>✓ Text report generated</p>';
                        }
                        status.style.backgroundColor = '#d4edda';
                    } else {
                        status.innerHTML = `<p>✗ Error: ${data.error}</p>`;
                        status.style.backgroundColor = '#f8d7da';
                    }
                } catch (error) {
                    status.innerHTML = `<p>✗ Error: ${error.message}</p>`;
                    status.style.backgroundColor = '#f8d7da';
                }
            }
            
            async function getQualityIssues() {
                const status = document.getElementById('qualityStatus');
                const details = document.getElementById('qualityDetails');
                
                status.style.display = 'block';
                status.innerHTML = '<p>Fetching quality issues...</p>';
                status.style.backgroundColor = '#e7f3fe';
                
                try {
                    const response = await fetch('/api/quality/issues');
                    const data = await response.json();
                    
                    if (data.success) {
                        status.innerHTML = `<p>✓ Found ${data.count} issues</p>`;
                        status.style.backgroundColor = data.count > 0 ? '#fff3cd' : '#d4edda';
                        
                        // Display issues
                        if (data.issues.length > 0) {
                            let html = '<h4>Quality Issues:</h4>';
                            
                            data.issues.forEach(issue => {
                                const severity = issue.severity || 'medium';
                                html += `
                                    <div class="issue-card issue-${severity}">
                                        <strong>${issue.type.replace('_', ' ').toUpperCase()}</strong>
                                        <p>${issue.message}</p>
                                        <small>Severity: ${severity.toUpperCase()}</small>
                                    </div>
                                `;
                            });
                            
                            details.innerHTML = html;
                        } else {
                            details.innerHTML = '<p>No quality issues found! 🎉</p>';
                        }
                    } else {
                        status.innerHTML = `<p>✗ Error: ${data.error}</p>`;
                        status.style.backgroundColor = '#f8d7da';
                    }
                } catch (error) {
                    status.innerHTML = `<p>✗ Error: ${error.message}</p>`;
                    status.style.backgroundColor = '#f8d7da';
                }
            }
            
            function displayQualityDashboard(analysis) {
                const dashboard = document.getElementById('qualityDashboard');
                const score = analysis.quality_score || 0;
                
                // Determine quality class
                let qualityClass = 'quality-very-poor';
                let qualityText = 'Very Poor';
                
                if (score >= 90) {
                    qualityClass = 'quality-excellent';
                    qualityText = 'Excellent';
                } else if (score >= 70) {
                    qualityClass = 'quality-good';
                    qualityText = 'Good';
                } else if (score >= 50) {
                    qualityClass = 'quality-fair';
                    qualityText = 'Fair';
                } else if (score >= 30) {
                    qualityClass = 'quality-poor';
                    qualityText = 'Poor';
                }
                
                let html = `
                    <div class="quality-score ${qualityClass}">
                        <h3>Overall Data Quality</h3>
                        <div style="font-size: 48px; font-weight: bold; margin: 10px 0;">${score.toFixed(1)}%</div>
                        <p><strong>${qualityText}</strong></p>
                    </div>
                    
                    <h4>Dimension Scores:</h4>
                `;
                
                // Add dimension scores
                const dimensions = ['completeness', 'validity', 'consistency', 'timeliness'];
                dimensions.forEach(dim => {
                    if (analysis[dim]) {
                        const score = analysis[dim].overall || 0;
                        html += `
                            <div class="dimension-card">
                                <strong>${dim.charAt(0).toUpperCase() + dim.slice(1)}</strong>
                                <div style="display: flex; justify-content: space-between; align-items: center;">
                                    <span>${score.toFixed(1)}%</span>
                                    <small>Score</small>
                                </div>
                                <div class="progress-container">
                                    <div class="progress-bar" style="width: ${score}%; background-color: ${getScoreColor(score)}"></div>
                                </div>
                            </div>
                        `;
                    }
                });
                
                dashboard.innerHTML = html;
            }
            
            function displayQualityMetrics(analysis) {
                const details = document.getElementById('qualityDetails');
                
                let html = '<h4>Detailed Metrics:</h4>';
                
                // Basic stats
                const stats = analysis.basic_stats || {};
                html += `
                    <div class="dimension-card">
                        <strong>Basic Statistics</strong>
                        <p>Total Records: ${stats.total_records || 0}</p>
                        <p>Unique Animals: ${stats.unique_animals || 0}</p>
                `;
                
                if (stats.date_range && stats.date_range.start) {
                    html += `<p>Date Range: ${stats.date_range.start} to ${stats.date_range.end}</p>`;
                }
                
                html += '</div>';
                
                // Completeness details
                const completeness = analysis.completeness || {};
                if (completeness.by_column) {
                    html += `
                        <div class="dimension-card">
                            <strong>Data Completeness</strong>
                            <p>Overall: ${completeness.overall?.toFixed(1) || 0}%</p>
                            <p>Missing values by column:</p>
                    `;
                    
                    Object.entries(completeness.by_column).forEach(([column, score]) => {
                        const missingPercent = 100 - (score || 0);
                        if (missingPercent > 0) {
                            html += `<p style="margin: 5px 0;">${column}: ${missingPercent.toFixed(1)}% missing</p>`;
                        }
                    });
                    
                    html += '</div>';
                }
                
                // Issues
                const issues = analysis.issues || [];
                if (issues.length > 0) {
                    html += '<div class="dimension-card"><strong>Issues Found:</strong>';
                    
                    issues.forEach(issue => {
                        const severity = issue.severity || 'medium';
                        html += `
                            <div class="issue-card issue-${severity}" style="margin: 10px 0;">
                                <strong>${issue.type.replace('_', ' ').toUpperCase()}</strong>
                                <p>${issue.message}</p>
                            </div>
                        `;
                    });
                    
                    html += '</div>';
                }
                
                details.innerHTML = html;
            }
            
            function getScoreColor(score) {
                if (score >= 90) return '#28a745';
                if (score >= 70) return '#ffc107';
                if (score >= 50) return '#fd7e14';
                if (score >= 30) return '#dc3545';
                return '#6c757d';
            }
            
            function showTextReport(reportText) {
                // Create modal dialog for text report
                const modal = document.createElement('div');
                modal.style.position = 'fixed';
                modal.style.top = '0';
                modal.style.left = '0';
                modal.style.width = '100%';
                modal.style.height = '100%';
                modal.style.backgroundColor = 'rgba(0,0,0,0.5)';
                modal.style.zIndex = '1000';
                modal.style.display = 'flex';
                modal.style.justifyContent = 'center';
                modal.style.alignItems = 'center';
                
                const content = document.createElement('div');
                content.style.backgroundColor = 'white';
                content.style.padding = '20px';
                content.style.borderRadius = '5px';
                content.style.maxWidth = '80%';
                content.style.maxHeight = '80%';
                content.style.overflow = 'auto';
                content.style.fontFamily = 'monospace';
                content.style.whiteSpace = 'pre-wrap';
                
                content.textContent = reportText;
                
                const closeBtn = document.createElement('button');
                closeBtn.textContent = 'Close';
                closeBtn.style.marginTop = '10px';
                closeBtn.style.padding = '5px 10px';
                closeBtn.onclick = function() {
                    document.body.removeChild(modal);
                };
                
                content.appendChild(document.createElement('br'));
                content.appendChild(closeBtn);
                modal.appendChild(content);
                document.body.appendChild(modal);
            }
            
            // Alert Log functions
            async function loadAlertLogs() {
                const logsDiv = document.getElementById('alertLogs');
                const statusDiv = document.getElementById('logStatus');
                
                logsDiv.innerHTML = '<p>Loading alert logs...</p>';
                statusDiv.style.display = 'none';
                
                try {
                    const response = await fetch('/api/logs/alerts?days=7');
                    const data = await response.json();
                    
                    if (data.success) {
                        if (data.alerts.length > 0) {
                            let html = `<p><strong>Recent Alerts (${data.alerts.length} total):</strong></p>`;
                            
                            data.alerts.slice(0, 20).forEach(alert => {
                                const severity = alert.severity || 'info';
                                const timestamp = alert.timestamp ? new Date(alert.timestamp).toLocaleString() : 'Unknown time';
                                const farm = alert.farm_id || 'Unknown farm';
                                const message = alert.message || 'No message';
                                
                                html += `
                                    <div class="log-entry log-${severity}">
                                        <strong>[${severity.toUpperCase()}] ${timestamp}</strong><br>
                                        Farm: ${farm} | Message: ${message}
                                    </div>
                                `;
                            });
                            
                            if (data.alerts.length > 20) {
                                html += `<p><em>... and ${data.alerts.length - 20} more alerts</em></p>`;
                            }
                            
                            logsDiv.innerHTML = html;
                        } else {
                            logsDiv.innerHTML = '<p>No alerts found in the last 7 days.</p>';
                        }
                        
                        // Also load stats
                        loadLogStats();
                    } else {
                        logsDiv.innerHTML = `<p>Error: ${data.error}</p>`;
                    }
                } catch (error) {
                    logsDiv.innerHTML = `<p>Error loading logs: ${error.message}</p>`;
                }
            }
            
            async function searchLogs() {
                const keyword = document.getElementById('searchKeyword').value;
                const severity = document.getElementById('searchSeverity').value;
                const logsDiv = document.getElementById('alertLogs');
                const statusDiv = document.getElementById('logStatus');
                
                logsDiv.innerHTML = '<p>Searching...</p>';
                statusDiv.style.display = 'none';
                
                try {
                    let url = `/api/logs/search?days=30`;
                    if (keyword) url += `&keyword=${encodeURIComponent(keyword)}`;
                    if (severity) url += `&severity=${severity}`;
                    
                    const response = await fetch(url);
                    const data = await response.json();
                    
                    if (data.success) {
                        if (data.results.length > 0) {
                            let html = `<p><strong>Search Results (${data.results.length} found):</strong></p>`;
                            
                            data.results.forEach(alert => {
                                const severity = alert.severity || 'info';
                                const timestamp = alert.timestamp ? new Date(alert.timestamp).toLocaleString() : 'Unknown time';
                                const farm = alert.farm_id || 'Unknown farm';
                                const message = alert.message || 'No message';
                                
                                html += `
                                    <div class="log-entry log-${severity}">
                                        <strong>[${severity.toUpperCase()}] ${timestamp}</strong><br>
                                        Farm: ${farm} | Message: ${message}
                                    </div>
                                `;
                            });
                            
                            logsDiv.innerHTML = html;
                            
                            statusDiv.style.display = 'block';
                            statusDiv.style.backgroundColor = '#d4edda';
                            statusDiv.innerHTML = `<p>✓ Found ${data.results.length} matching alerts</p>`;
                        } else {
                            logsDiv.innerHTML = '<p>No alerts match your search criteria.</p>';
                            
                            statusDiv.style.display = 'block';
                            statusDiv.style.backgroundColor = '#fff3cd';
                            statusDiv.innerHTML = '<p>No matching alerts found.</p>';
                        }
                    } else {
                        logsDiv.innerHTML = `<p>Error: ${data.error}</p>`;
                    }
                } catch (error) {
                    logsDiv.innerHTML = `<p>Error searching logs: ${error.message}</p>`;
                }
            }
            
            async function loadLogStats() {
                const statsDiv = document.getElementById('logStats');
                
                try {
                    const response = await fetch('/api/logs/stats?days=7');
                    const data = await response.json();
                    
                    if (data.success) {
                        const stats = data.stats;
                        let html = '<strong>Last 7 Days Statistics:</strong><br>';
                        
                        html += `
                            <div class="stat-card">
                                <div style="font-size: 24px; font-weight: bold;">${stats.total_alerts || 0}</div>
                                <div>Total Alerts</div>
                            </div>
                        `;
                        
                        // Severity breakdown
                        if (stats.by_severity) {
                            Object.entries(stats.by_severity).forEach(([severity, count]) => {
                                html += `
                                    <div class="stat-card">
                                        <div style="font-size: 20px; font-weight: bold;">${count}</div>
                                        <div>${severity.toUpperCase()}</div>
                                    </div>
                                `;
                            });
                        }
                        
                        statsDiv.innerHTML = html;
                    }
                } catch (error) {
                    statsDiv.innerHTML = `<p>Error loading stats: ${error.message}</p>`;
                }
            }
            
            async function exportLogs(format) {
                const statusDiv = document.getElementById('logStatus');
                
                statusDiv.style.display = 'block';
                statusDiv.innerHTML = `<p>Exporting logs as ${format.toUpperCase()}...</p>`;
                statusDiv.style.backgroundColor = '#e7f3fe';
                
                try {
                    const response = await fetch(`/api/logs/export?days=30&format=${format}`);
                    const data = await response.json();
                    
                    if (data.success) {
                        statusDiv.innerHTML = `<p>✓ Export complete! <a href="/api/exports/download/${data.filename}" target="_blank">Download ${data.filename}</a></p>`;
                        statusDiv.style.backgroundColor = '#d4edda';
                    } else {
                        statusDiv.innerHTML = `<p>✗ Export failed: ${data.error}</p>`;
                        statusDiv.style.backgroundColor = '#f8d7da';
                    }
                } catch (error) {
                    statusDiv.innerHTML = `<p>✗ Error: ${error.message}</p>`;
                    statusDiv.style.backgroundColor = '#f8d7da';
                }
            }
            
            // Load quality score on page load
            async function loadQualityScore() {
                try {
                    const response = await fetch('/api/quality/score');
                    const data = await response.json();
                    
                    if (data.success) {
                        // You could display the score in a small widget
                        console.log(`Data quality score: ${data.score}% (${data.rating.text})`);
                    }
                } catch (error) {
                    console.log('Could not load quality score:', error.message);
                }
            }
            
            // Initialize everything on page load
            document.addEventListener('DOMContentLoaded', function() {
                loadData();
                loadExports();
                loadAlertLogs();
                loadQualityScore();
                
                // Refresh every 5 minutes
                setInterval(loadData, 300000);
            });
        </script>
    </body>
    </html>
    """
    
    with open('templates/index.html', 'w', encoding='utf-8') as f:
        f.write(html_template)
    
    print("Starting dashboard server on http://localhost:5000")
    print("Press Ctrl+C to stop")
    app.run(debug=True, host='0.0.0.0', port=5000)