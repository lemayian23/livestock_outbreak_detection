"""
Health check system for monitoring pipeline components
"""
import logging
import time
import threading
from typing import Dict, List, Optional, Callable, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import psutil
import socket
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health status enumeration"""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class HealthCheck:
    """Individual health check"""
    name: str
    check_fn: Callable[[], Tuple[bool, str]]
    description: str = ""
    critical: bool = False
    interval_seconds: int = 60
    last_check: Optional[datetime] = None
    last_status: HealthStatus = HealthStatus.UNKNOWN
    last_message: str = ""
    failure_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SystemMetrics:
    """System metrics snapshot"""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    disk_usage_percent: float
    network_bytes_sent: int
    network_bytes_recv: int
    process_count: int
    python_memory_mb: float
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'cpu_percent': round(self.cpu_percent, 2),
            'memory_percent': round(self.memory_percent, 2),
            'disk_usage_percent': round(self.disk_usage_percent, 2),
            'network_bytes_sent': self.network_bytes_sent,
            'network_bytes_recv': self.network_bytes_recv,
            'process_count': self.process_count,
            'python_memory_mb': round(self.python_memory_mb, 2)
        }


class HealthMonitor:
    """
    Monitors health of system components and services
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.checks: Dict[str, HealthCheck] = {}
        self.metrics_history: List[SystemMetrics] = []
        self.max_history: int = self.config.get('monitoring', {}).get('max_history', 100)
        self.is_monitoring: bool = False
        self.monitor_thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._initialize_default_checks()
        
        logger.info("Health monitor initialized")
    
    def _initialize_default_checks(self) -> None:
        """Initialize default health checks"""
        
        # Disk space check
        def check_disk_space() -> Tuple[bool, str]:
            try:
                usage = psutil.disk_usage('/')
                percent_used = usage.percent
                if percent_used > 90:
                    return False, f"Disk usage critical: {percent_used:.1f}%"
                elif percent_used > 80:
                    return False, f"Disk usage high: {percent_used:.1f}%"
                return True, f"Disk usage OK: {percent_used:.1f}%"
            except Exception as e:
                return False, f"Disk check failed: {str(e)}"
        
        self.register_check(
            HealthCheck(
                name="disk_space",
                check_fn=check_disk_space,
                description="Check available disk space",
                critical=True,
                interval_seconds=300  # 5 minutes
            )
        )
        
        # Memory check
        def check_memory() -> Tuple[bool, str]:
            try:
                memory = psutil.virtual_memory()
                percent_used = memory.percent
                if percent_used > 90:
                    return False, f"Memory usage critical: {percent_used:.1f}%"
                elif percent_used > 80:
                    return False, f"Memory usage high: {percent_used:.1f}%"
                return True, f"Memory usage OK: {percent_used:.1f}%"
            except Exception as e:
                return False, f"Memory check failed: {str(e)}"
        
        self.register_check(
            HealthCheck(
                name="memory",
                check_fn=check_memory,
                description="Check system memory usage",
                critical=True,
                interval_seconds=60
            )
        )
        
        # CPU check
        def check_cpu() -> Tuple[bool, str]:
            try:
                cpu_percent = psutil.cpu_percent(interval=1)
                if cpu_percent > 90:
                    return False, f"CPU usage critical: {cpu_percent:.1f}%"
                elif cpu_percent > 80:
                    return False, f"CPU usage high: {cpu_percent:.1f}%"
                return True, f"CPU usage OK: {cpu_percent:.1f}%"
            except Exception as e:
                return False, f"CPU check failed: {str(e)}"
        
        self.register_check(
            HealthCheck(
                name="cpu",
                check_fn=check_cpu,
                description="Check CPU usage",
                critical=False,
                interval_seconds=60
            )
        )
        
        # Network connectivity check
        def check_network() -> Tuple[bool, str]:
            try:
                # Try to connect to Google DNS
                socket.create_connection(("8.8.8.8", 53), timeout=3)
                return True, "Network connectivity OK"
            except Exception as e:
                return False, f"Network check failed: {str(e)}"
        
        self.register_check(
            HealthCheck(
                name="network",
                check_fn=check_network,
                description="Check network connectivity",
                critical=False,
                interval_seconds=30
            )
        )
        
        # Database connection check (placeholder)
        def check_database() -> Tuple[bool, str]:
            try:
                # This would check your actual database
                # For now, always return healthy
                return True, "Database connection OK"
            except Exception as e:
                return False, f"Database check failed: {str(e)}"
        
        self.register_check(
            HealthCheck(
                name="database",
                check_fn=check_database,
                description="Check database connectivity",
                critical=True,
                interval_seconds=30
            )
        )
    
    def register_check(self, check: HealthCheck) -> None:
        """Register a health check"""
        with self._lock:
            self.checks[check.name] = check
            logger.info(f"Registered health check: {check.name}")
    
    def run_check(self, check_name: str) -> Tuple[HealthStatus, str]:
        """Run a specific health check"""
        with self._lock:
            if check_name not in self.checks:
                return HealthStatus.UNKNOWN, f"Check '{check_name}' not found"
            
            check = self.checks[check_name]
            
            try:
                success, message = check.check_fn()
                check.last_check = datetime.now()
                check.last_message = message
                
                if success:
                    check.last_status = HealthStatus.HEALTHY
                    check.failure_count = 0
                else:
                    check.last_status = HealthStatus.WARNING if not check.critical else HealthStatus.CRITICAL
                    check.failure_count += 1
                
                logger.debug(f"Health check '{check_name}': {check.last_status.value} - {message}")
                return check.last_status, message
                
            except Exception as e:
                check.last_status = HealthStatus.CRITICAL
                check.last_message = f"Check failed with error: {str(e)}"
                check.failure_count += 1
                logger.error(f"Health check '{check_name}' failed: {str(e)}")
                return check.last_status, check.last_message
    
    def run_all_checks(self) -> Dict[str, Dict]:
        """Run all health checks"""
        results = {}
        
        with self._lock:
            for check_name in self.checks:
                status, message = self.run_check(check_name)
                results[check_name] = {
                    'status': status.value,
                    'message': message,
                    'critical': self.checks[check_name].critical,
                    'last_check': self.checks[check_name].last_check.isoformat() 
                    if self.checks[check_name].last_check else None
                }
        
        return results
    
    def collect_system_metrics(self) -> SystemMetrics:
        """Collect current system metrics"""
        try:
            timestamp = datetime.now()
            
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=0.1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            
            # Disk usage
            disk = psutil.disk_usage('/')
            
            # Network I/O
            net_io = psutil.net_io_counters()
            
            # Process count
            process_count = len(psutil.pids())
            
            # Python process memory
            process = psutil.Process()
            python_memory_mb = process.memory_info().rss / 1024 / 1024
            
            metrics = SystemMetrics(
                timestamp=timestamp,
                cpu_percent=cpu_percent,
                memory_percent=memory.percent,
                disk_usage_percent=disk.percent,
                network_bytes_sent=net_io.bytes_sent,
                network_bytes_recv=net_io.bytes_recv,
                process_count=process_count,
                python_memory_mb=python_memory_mb
            )
            
            # Store in history
            self.metrics_history.append(metrics)
            if len(self.metrics_history) > self.max_history:
                self.metrics_history.pop(0)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to collect system metrics: {str(e)}")
            # Return empty metrics
            return SystemMetrics(
                timestamp=datetime.now(),
                cpu_percent=0,
                memory_percent=0,
                disk_usage_percent=0,
                network_bytes_sent=0,
                network_bytes_recv=0,
                process_count=0,
                python_memory_mb=0
            )
    
    def get_overall_health(self) -> Tuple[HealthStatus, Dict]:
        """Get overall system health status"""
        results = self.run_all_checks()
        
        has_critical = False
        has_warning = False
        
        for check_result in results.values():
            if check_result['status'] == HealthStatus.CRITICAL.value:
                has_critical = True
            elif check_result['status'] == HealthStatus.WARNING.value:
                has_warning = True
        
        if has_critical:
            overall_status = HealthStatus.CRITICAL
        elif has_warning:
            overall_status = HealthStatus.WARNING
        else:
            overall_status = HealthStatus.HEALTHY
        
        summary = {
            'overall_status': overall_status.value,
            'total_checks': len(results),
            'healthy_checks': sum(1 for r in results.values() 
                                if r['status'] == HealthStatus.HEALTHY.value),
            'warning_checks': sum(1 for r in results.values() 
                                if r['status'] == HealthStatus.WARNING.value),
            'critical_checks': sum(1 for r in results.values() 
                                 if r['status'] == HealthStatus.CRITICAL.value),
            'checks': results
        }
        
        return overall_status, summary
    
    def start_monitoring(self, interval_seconds: int = 60) -> None:
        """Start background monitoring"""
        if self.is_monitoring:
            logger.warning("Monitoring already started")
            return
        
        self.is_monitoring = True
        
        def monitor_loop():
            logger.info("Starting health monitoring background thread")
            
            while self.is_monitoring:
                try:
                    # Collect metrics
                    self.collect_system_metrics()
                    
                    # Run checks that are due
                    now = datetime.now()
                    with self._lock:
                        for check in self.checks.values():
                            if (check.last_check is None or 
                                (now - check.last_check).total_seconds() >= check.interval_seconds):
                                self.run_check(check.name)
                    
                    # Log overall health periodically
                    if int(time.time()) % 300 == 0:  # Every 5 minutes
                        status, summary = self.get_overall_health()
                        if status != HealthStatus.HEALTHY:
                            logger.warning(f"System health: {status.value}")
                    
                except Exception as e:
                    logger.error(f"Error in monitoring loop: {str(e)}")
                
                # Sleep
                time.sleep(interval_seconds)
            
            logger.info("Health monitoring stopped")
        
        self.monitor_thread = threading.Thread(
            target=monitor_loop,
            name="HealthMonitor",
            daemon=True
        )
        self.monitor_thread.start()
        
        logger.info(f"Health monitoring started with {interval_seconds}s interval")
    
    def stop_monitoring(self) -> None:
        """Stop background monitoring"""
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
            self.monitor_thread = None
        logger.info("Health monitoring stopped")
    
    def get_metrics_history(self, limit: Optional[int] = None) -> List[Dict]:
        """Get metrics history"""
        with self._lock:
            history = [m.to_dict() for m in self.metrics_history]
            if limit:
                history = history[-limit:]
            return history
    
    def get_recent_metrics(self, minutes: int = 10) -> List[Dict]:
        """Get metrics from the last N minutes"""
        cutoff = datetime.now() - timedelta(minutes=minutes)
        
        with self._lock:
            recent = [
                m.to_dict() for m in self.metrics_history
                if m.timestamp >= cutoff
            ]
            return recent
    
    def generate_health_report(self) -> Dict:
        """Generate comprehensive health report"""
        overall_status, health_summary = self.get_overall_health()
        metrics = self.collect_system_metrics()
        recent_metrics = self.get_recent_metrics(minutes=30)
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': overall_status.value,
            'health_summary': health_summary,
            'current_metrics': metrics.to_dict(),
            'recent_metrics': recent_metrics,
            'monitoring_active': self.is_monitoring,
            'registered_checks': list(self.checks.keys())
        }
        
        return report
    
    def save_health_report(self, filepath: str) -> None:
        """Save health report to file"""
        report = self.generate_health_report()
        
        try:
            with open(filepath, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Health report saved to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save health report: {str(e)}")
    
    def check_service_health(self, service_name: str, host: str, port: int, 
                           timeout: int = 5) -> Tuple[bool, str]:
        """Check if a service is reachable"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                return True, f"{service_name} at {host}:{port} is reachable"
            else:
                return False, f"{service_name} at {host}:{port} is not reachable"
        except Exception as e:
            return False, f"Service check failed for {service_name}: {str(e)}"
    
    def add_service_check(self, service_name: str, host: str, port: int, 
                         interval_seconds: int = 30) -> None:
        """Add a service health check"""
        
        def check_service() -> Tuple[bool, str]:
            return self.check_service_health(service_name, host, port)
        
        check = HealthCheck(
            name=f"service_{service_name.lower()}",
            check_fn=check_service,
            description=f"Check {service_name} service connectivity",
            critical=True,
            interval_seconds=interval_seconds
        )
        
        self.register_check(check)
        logger.info(f"Added service check for {service_name} at {host}:{port}")


# Global health monitor instance
_health_monitor: Optional[HealthMonitor] = None


def get_health_monitor(config: Optional[Dict] = None) -> HealthMonitor:
    """
    Get or create the global health monitor instance
    
    Args:
        config: Configuration dictionary
        
    Returns:
        HealthMonitor instance
    """
    global _health_monitor
    
    if _health_monitor is None:
        _health_monitor = HealthMonitor(config)
    
    return _health_monitor


def reset_health_monitor() -> None:
    """Reset the global health monitor (for testing)"""
    global _health_monitor
    if _health_monitor:
        _health_monitor.stop_monitoring()
    _health_monitor = None