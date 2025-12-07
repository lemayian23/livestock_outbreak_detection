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


<!-- Add this after the export section -->
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

<style>
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
</style>

<script>
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
    
    // Call on page load
    document.addEventListener('DOMContentLoaded', function() {
        loadQualityScore();
    });
</script>



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