"""
Simple alert logger for tracking all system alerts
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import csv


class AlertLogger:
    """Simple logger for tracking alerts in text files"""
    
    def __init__(self, log_dir: str = './outputs/alert_logs'):
        """
        Initialize alert logger
        
        Args:
            log_dir: Directory to store log files
        """
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
    
    def log_alert(self, alert_data: Dict):
        """
        Log an alert to the daily log file
        
        Args:
            alert_data: Dictionary containing alert information
        """
        # Ensure required fields
        if 'timestamp' not in alert_data:
            alert_data['timestamp'] = datetime.now().isoformat()
        
        if 'severity' not in alert_data:
            alert_data['severity'] = 'info'
        
        # Get today's log file
        today = datetime.now().strftime('%Y-%m-%d')
        log_file = os.path.join(self.log_dir, f'alerts_{today}.log')
        
        # Format log entry
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        severity = alert_data.get('severity', 'INFO').upper()
        farm_id = alert_data.get('farm_id', 'UNKNOWN')
        message = alert_data.get('message', 'No message')
        affected = alert_data.get('affected_animals', 0)
        
        log_entry = f"[{timestamp}] [{severity}] Farm: {farm_id} | Animals: {affected} | {message}\n"
        
        # Write to log file
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry)
        
        # Also save detailed JSON log
        self._save_json_log(alert_data)
        
        print(f"✓ Alert logged: {severity} - {message}")
    
    def _save_json_log(self, alert_data: Dict):
        """Save detailed alert data to JSON log"""
        today = datetime.now().strftime('%Y-%m-%d')
        json_file = os.path.join(self.log_dir, f'details_{today}.json')
        
        # Read existing logs if file exists
        existing_logs = []
        if os.path.exists(json_file):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    existing_logs = json.load(f)
            except:
                existing_logs = []
        
        # Add new alert
        existing_logs.append(alert_data)
        
        # Save back to file
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(existing_logs, f, indent=2, default=str)
    
    def get_todays_alerts(self) -> List[str]:
        """Get today's alerts as text lines"""
        today = datetime.now().strftime('%Y-%m-%d')
        log_file = os.path.join(self.log_dir, f'alerts_{today}.log')
        
        if not os.path.exists(log_file):
            return []
        
        with open(log_file, 'r', encoding='utf-8') as f:
            return f.readlines()
    
    def get_recent_alerts(self, days: int = 7) -> List[Dict]:
        """
        Get alerts from the last N days
        
        Args:
            days: Number of days to look back
            
        Returns:
            List of alert dictionaries
        """
        alerts = []
        
        for i in range(days):
            date = datetime.now() - timedelta(days=i)
            date_str = date.strftime('%Y-%m-%d')
            json_file = os.path.join(self.log_dir, f'details_{date_str}.json')
            
            if os.path.exists(json_file):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        day_alerts = json.load(f)
                        alerts.extend(day_alerts)
                except:
                    continue
        
        return alerts
    
    def search_alerts(self, 
                     keyword: str = None,
                     severity: str = None,
                     farm_id: str = None,
                     days: int = 30) -> List[Dict]:
        """
        Search alerts with filters
        
        Args:
            keyword: Text to search in alert messages
            severity: Filter by severity level
            farm_id: Filter by farm ID
            days: Number of days to search
            
        Returns:
            List of matching alerts
        """
        all_alerts = self.get_recent_alerts(days)
        filtered = []
        
        for alert in all_alerts:
            # Apply filters
            match = True
            
            if severity and alert.get('severity', '').lower() != severity.lower():
                match = False
            
            if farm_id and alert.get('farm_id', '').lower() != farm_id.lower():
                match = False
            
            if keyword:
                keyword_lower = keyword.lower()
                found = False
                
                # Search in message
                if keyword_lower in alert.get('message', '').lower():
                    found = True
                
                # Search in description
                if not found and 'description' in alert:
                    if keyword_lower in alert['description'].lower():
                        found = True
                
                if not found:
                    match = False
            
            if match:
                filtered.append(alert)
        
        return filtered
    
    def get_alert_stats(self, days: int = 7) -> Dict:
        """
        Get statistics about alerts
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with alert statistics
        """
        alerts = self.get_recent_alerts(days)
        
        if not alerts:
            return {
                'total_alerts': 0,
                'by_severity': {},
                'by_farm': {},
                'daily_count': {}
            }
        
        stats = {
            'total_alerts': len(alerts),
            'by_severity': {},
            'by_farm': {},
            'daily_count': {}
        }
        
        # Count by severity
        for alert in alerts:
            severity = alert.get('severity', 'unknown')
            stats['by_severity'][severity] = stats['by_severity'].get(severity, 0) + 1
            
            # Count by farm
            farm = alert.get('farm_id', 'unknown')
            stats['by_farm'][farm] = stats['by_farm'].get(farm, 0) + 1
            
            # Count by date
            if 'timestamp' in alert:
                try:
                    alert_date = datetime.fromisoformat(alert['timestamp']).strftime('%Y-%m-%d')
                    stats['daily_count'][alert_date] = stats['daily_count'].get(alert_date, 0) + 1
                except:
                    pass
        
        return stats
    
    def export_alerts(self, 
                     days: int = 30,
                     format: str = 'csv') -> str:
        """
        Export alerts to file
        
        Args:
            days: Number of days to export
            format: Export format ('csv' or 'json')
            
        Returns:
            Path to exported file
        """
        alerts = self.get_recent_alerts(days)
        
        if not alerts:
            return None
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if format == 'csv':
            filename = f'alerts_export_{timestamp}.csv'
            filepath = os.path.join(self.log_dir, filename)
            
            # Write CSV
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                if alerts:
                    fieldnames = alerts[0].keys()
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(alerts)
            
            return filepath
        
        elif format == 'json':
            filename = f'alerts_export_{timestamp}.json'
            filepath = os.path.join(self.log_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(alerts, f, indent=2, default=str)
            
            return filepath
        
        return None
    
    def cleanup_old_logs(self, days_to_keep: int = 90) -> int:
        """
        Delete log files older than specified days
        
        Args:
            days_to_keep: Keep files newer than this many days
            
        Returns:
            Number of files deleted
        """
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        deleted_count = 0
        
        for filename in os.listdir(self.log_dir):
            filepath = os.path.join(self.log_dir, filename)
            
            if os.path.isfile(filepath):
                file_mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
                
                if file_mtime < cutoff_date:
                    try:
                        os.remove(filepath)
                        deleted_count += 1
                    except:
                        pass
        
        return deleted_count
    
    def get_log_summary(self) -> str:
        """Get summary of log files"""
        if not os.path.exists(self.log_dir):
            return "No log directory found"
        
        files = os.listdir(self.log_dir)
        
        if not files:
            return "No log files found"
        
        # Count files by type
        log_files = [f for f in files if f.endswith('.log')]
        json_files = [f for f in files if f.endswith('.json')]
        export_files = [f for f in files if 'export' in f]
        
        # Get total size
        total_size = 0
        for f in files:
            filepath = os.path.join(self.log_dir, f)
            total_size += os.path.getsize(filepath)
        
        summary = f"""
        Alert Log Summary:
        ==================
        Total files: {len(files)}
        - Log files: {len(log_files)}
        - JSON files: {len(json_files)}
        - Export files: {len(export_files)}
        Total size: {total_size / 1024:.1f} KB
        
        Recent log files:
        """
        
        # List recent files
        recent_files = sorted(files, reverse=True)[:5]
        for f in recent_files:
            filepath = os.path.join(self.log_dir, f)
            size = os.path.getsize(filepath)
            mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
            summary += f"\n  {f} ({size/1024:.1f} KB, {mtime.strftime('%Y-%m-%d')})"
        
        return summary