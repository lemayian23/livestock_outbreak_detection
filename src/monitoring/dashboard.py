"""
Simple monitoring dashboard for health checks
"""
import json
from typing import Dict, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class MonitoringDashboard:
    """Simple text-based monitoring dashboard"""
    
    def __init__(self, health_monitor):
        self.health_monitor = health_monitor
    
    def display_health_status(self) -> str:
        """Display health status in a formatted way"""
        status, summary = self.health_monitor.get_overall_health()
        metrics = self.health_monitor.collect_system_metrics()
        
        # Colors for terminal (ANSI codes)
        colors = {
            'healthy': '\033[92m',  # Green
            'warning': '\033[93m',  # Yellow
            'critical': '\033[91m', # Red
            'unknown': '\033[90m',  # Gray
            'reset': '\033[0m'      # Reset
        }
        
        status_color = {
            'healthy': colors['healthy'],
            'warning': colors['warning'],
            'critical': colors['critical'],
            'unknown': colors['unknown']
        }
        
        output = []
        output.append("=" * 60)
        output.append("SYSTEM HEALTH DASHBOARD")
        output.append("=" * 60)
        output.append("")
        
        # Overall status
        color = status_color.get(status.value, colors['unknown'])
        output.append(f"Overall Status: {color}{status.value.upper()}{colors['reset']}")
        output.append(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output.append("")
        
        # Health checks
        output.append("HEALTH CHECKS:")
        output.append("-" * 40)
        
        for check_name, check_info in summary.get('checks', {}).items():
            status = check_info['status']
            message = check_info['message']
            critical = check_info['critical']
            
            color = status_color.get(status, colors['unknown'])
            symbol = "✓" if status == 'healthy' else "✗" if status == 'critical' else "!"
            
            critical_mark = " [CRITICAL]" if critical else ""
            
            output.append(f"  {symbol} {check_name:20} {color}{status.upper():10}{colors['reset']}{critical_mark}")
            output.append(f"      {message}")
        
        output.append("")
        
        # System metrics
        output.append("SYSTEM METRICS:")
        output.append("-" * 40)
        output.append(f"  CPU Usage:    {metrics.cpu_percent:6.1f}%")
        output.append(f"  Memory Usage: {metrics.memory_percent:6.1f}%")
        output.append(f"  Disk Usage:   {metrics.disk_usage_percent:6.1f}%")
        output.append(f"  Processes:    {metrics.process_count:6}")
        output.append(f"  Python Memory:{metrics.python_memory_mb:6.1f} MB")
        
        output.append("")
        
        # History summary
        recent = self.health_monitor.get_recent_metrics(minutes=5)
        if recent:
            cpu_values = [m['cpu_percent'] for m in recent]
            mem_values = [m['memory_percent'] for m in recent]
            
            if cpu_values:
                output.append(f"  CPU (5min):   {min(cpu_values):4.1f}% - {max(cpu_values):4.1f}% avg")
            if mem_values:
                output.append(f"  Memory (5min):{min(mem_values):4.1f}% - {max(mem_values):4.1f}% avg")
        
        output.append("=" * 60)
        
        return "\n".join(output)
    
    def display_json_report(self) -> str:
        """Display health report as JSON"""
        report = self.health_monitor.generate_health_report()
        return json.dumps(report, indent=2, default=str)
    
    def display_metrics_history(self, limit: int = 10) -> str:
        """Display metrics history"""
        history = self.health_monitor.get_metrics_history(limit=limit)
        
        output = []
        output.append("METRICS HISTORY:")
        output.append("-" * 60)
        
        for metrics in history:
            timestamp = metrics['timestamp'][11:19]  # Just time
            output.append(f"{timestamp} | CPU: {metrics['cpu_percent']:5.1f}% | "
                         f"Mem: {metrics['memory_percent']:5.1f}% | "
                         f"Disk: {metrics['disk_usage_percent']:5.1f}%")
        
        return "\n".join(output)
    
    def check_and_alert(self) -> bool:
        """
        Check health and return True if system is healthy
        
        Returns:
            bool: True if system is healthy, False if warnings/critical issues
        """
        status, summary = self.health_monitor.get_overall_health()
        
        if status.value == 'critical':
            logger.error("CRITICAL system health issues detected!")
            for check_name, check_info in summary['checks'].items():
                if check_info['status'] == 'critical':
                    logger.error(f"  - {check_name}: {check_info['message']}")
            return False
        
        elif status.value == 'warning':
            logger.warning("Warning: system has health warnings")
            for check_name, check_info in summary['checks'].items():
                if check_info['status'] == 'warning':
                    logger.warning(f"  - {check_name}: {check_info['message']}")
            return True  # Still considered healthy enough to run
        
        else:
            logger.info("System health: OK")
            return True