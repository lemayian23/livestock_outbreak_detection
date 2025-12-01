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