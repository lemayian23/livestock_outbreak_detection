"""
Log analysis utilities for monitoring and debugging
"""
import json
import re
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from datetime import datetime, timedelta
import gzip
from collections import Counter, defaultdict

# Import with alias
import logging as std_logging

logger = std_logging.getLogger(__name__)

                   
class LogAnalyzer:
    """Analyze log files for patterns, errors, and statistics"""
    
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_pattern = re.compile(r'\{"timestamp".*\}')
    
    def parse_log_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse a JSON log line"""
        try:
            # Try to parse as JSON
            return json.loads(line.strip())
        except json.JSONDecodeError:
            # Try to find JSON in line (in case of mixed output)
            match = self.log_pattern.search(line)
            if match:
                try:
                    return json.loads(match.group())
                except json.JSONDecodeError:
                    return None
            return None
    
    def read_logs(self, 
                 log_file: Optional[str] = None,
                 since: Optional[datetime] = None,
                 until: Optional[datetime] = None,
                 level: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Read and parse log files
        
        Args:
            log_file: Specific log file to read (None for all)
            since: Only include logs after this time
            until: Only include logs before this time
            level: Only include logs of this level or higher
            
        Returns:
            List of parsed log entries
        """
        logs = []
        
        # Determine which files to read
        if log_file:
            files = [Path(log_file)]
        else:
            # Get all log files in directory
            files = list(self.log_dir.glob("*.log"))
            files += list(self.log_dir.glob("*.log.*"))  # Rotated logs
        
        level_priority = {
            'DEBUG': 10,
            'INFO': 20,
            'WARNING': 30,
            'ERROR': 40,
            'CRITICAL': 50
        }
        min_priority = level_priority.get(level, 0) if level else 0
        
        for file_path in files:
            if not file_path.exists():
                continue
            
            # Handle gzipped rotated logs
            open_func = gzip.open if file_path.suffix == '.gz' else open
            open_mode = 'rt' if file_path.suffix == '.gz' else 'r'
            
            try:
                with open_func(file_path, open_mode, encoding='utf-8') as f:
                    for line in f:
                        log_entry = self.parse_log_line(line)
                        if not log_entry:
                            continue
                        
                        # Apply filters
                        if since:
                            log_time = datetime.fromisoformat(
                                log_entry['timestamp'].replace('Z', '+00:00')
                            )
                            if log_time < since:
                                continue
                        
                        if until:
                            log_time = datetime.fromisoformat(
                                log_entry['timestamp'].replace('Z', '+00:00')
                            )
                            if log_time > until:
                                continue
                        
                        if level:
                            entry_priority = level_priority.get(log_entry.get('level', 'DEBUG'), 0)
                            if entry_priority < min_priority:
                                continue
                        
                        logs.append(log_entry)
                        
            except Exception as e:
                logger.error(f"Failed to read log file {file_path}: {str(e)}")
                continue
        
        return logs
    
    def analyze_errors(self, 
                      hours: int = 24,
                      group_by: str = 'message') -> Dict[str, Any]:
        """
        Analyze error logs
        
        Args:
            hours: Number of hours to look back
            group_by: How to group errors ('message', 'module', 'function')
            
        Returns:
            Error analysis report
        """
        since = datetime.utcnow() - timedelta(hours=hours)
        
        # Read error logs
        error_logs = self.read_logs(
            since=since,
            level='ERROR'
        )
        
        # Group errors
        groups = defaultdict(list)
        for log in error_logs:
            if group_by == 'message':
                key = log.get('message', 'Unknown')
            elif group_by == 'module':
                key = log.get('module', 'Unknown')
            elif group_by == 'function':
                key = log.get('function', 'Unknown')
            else:
                key = log.get('message', 'Unknown')
            
            groups[key].append(log)
        
        # Create analysis
        analysis = {
            'total_errors': len(error_logs),
            'time_period_hours': hours,
            'error_distribution': {},
            'most_common_errors': [],
            'recent_errors': error_logs[-10:] if error_logs else []
        }
        
        # Calculate distribution
        for key, entries in groups.items():
            analysis['error_distribution'][key] = len(entries)
        
        # Get most common errors
        error_counter = Counter([log.get('message', 'Unknown') for log in error_logs])
        analysis['most_common_errors'] = error_counter.most_common(10)
        
        return analysis
    
    def performance_report(self, hours: int = 1) -> Dict[str, Any]:
        """
        Generate performance report from timing logs
        
        Returns:
            Performance metrics
        """
        since = datetime.utcnow() - timedelta(hours=hours)
        
        # Read all logs
        logs = self.read_logs(since=since)
        
        # Extract timing information
        operations = defaultdict(list)
        
        for log in logs:
            if 'operation' in log and 'duration_ms' in log:
                operation = log['operation']
                duration = log['duration_ms']
                operations[operation].append(duration)
        
        # Calculate statistics
        performance = {}
        for operation, durations in operations.items():
            if durations:
                performance[operation] = {
                    'count': len(durations),
                    'avg_ms': sum(durations) / len(durations),
                    'min_ms': min(durations),
                    'max_ms': max(durations),
                    'p95_ms': sorted(durations)[int(len(durations) * 0.95)]
                }
        
        return {
            'time_period_hours': hours,
            'total_timed_operations': sum(len(d) for d in operations.values()),
            'operations': performance,
            'slowest_operations': sorted(
                performance.items(),
                key=lambda x: x[1]['avg_ms'],
                reverse=True
            )[:5]
        }
    
    def search_logs(self, 
                   query: str,
                   hours: int = 24,
                   case_sensitive: bool = False) -> List[Dict[str, Any]]:
        """
        Search logs for specific text
        
        Args:
            query: Search query
            hours: Hours to look back
            case_sensitive: Whether search is case sensitive
            
        Returns:
            Matching log entries
        """
        since = datetime.utcnow() - timedelta(hours=hours)
        
        # Read logs
        logs = self.read_logs(since=since)
        
        # Prepare search
        if not case_sensitive:
            query = query.lower()
        
        # Search in logs
        results = []
        for log in logs:
            # Convert log to string for searching
            log_str = json.dumps(log, default=str)
            
            if not case_sensitive:
                log_str = log_str.lower()
            
            if query in log_str:
                results.append(log)
        
        return results
    
    def log_summary(self, hours: int = 1) -> Dict[str, Any]:
        """
        Generate summary of recent logs
        
        Returns:
            Log summary
        """
        since = datetime.utcnow() - timedelta(hours=hours)
        
        # Read logs
        logs = self.read_logs(since=since)
        
        if not logs:
            return {'total_logs': 0, 'message': 'No logs found in specified period'}
        
        # Count by level
        level_counts = Counter(log.get('level', 'UNKNOWN') for log in logs)
        
        # Get unique loggers
        loggers = set(log.get('logger', 'UNKNOWN') for log in logs)
        
        # Get time range
        timestamps = [log.get('timestamp') for log in logs if 'timestamp' in log]
        if timestamps:
            time_range = {
                'first': min(timestamps),
                'last': max(timestamps)
            }
        else:
            time_range = None
        
        return {
            'total_logs': len(logs),
            'time_period_hours': hours,
            'level_distribution': dict(level_counts),
            'unique_loggers': list(loggers),
            'time_range': time_range,
            'recent_messages': [log.get('message') for log in logs[-5:]]
        }
    
    def export_logs(self, 
                   output_file: str,
                   hours: int = 24,
                   format: str = 'json') -> None:
        """
        Export logs to file
        
        Args:
            output_file: Output file path
            hours: Hours to export
            format: Export format ('json' or 'csv')
        """
        since = datetime.utcnow() - timedelta(hours=hours)
        logs = self.read_logs(since=since)
        
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if format.lower() == 'json':
            with open(output_path, 'w') as f:
                json.dump(logs, f, indent=2, default=str)
        
        elif format.lower() == 'csv':
            import csv
            
            # Extract all possible fields
            fields = set()
            for log in logs:
                fields.update(log.keys())
            
            with open(output_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=list(fields))
                writer.writeheader()
                for log in logs:
                    writer.writerow(log)
        
        logger.info(f"Exported {len(logs)} logs to {output_file}")
    
    def cleanup_old_logs(self, days: int = 30) -> Tuple[int, List[str]]:
        """
        Clean up old log files
        
        Args:
            days: Delete logs older than this many days
            
        Returns:
            Tuple of (files_deleted, list_of_deleted_files)
        """
        cutoff = datetime.utcnow() - timedelta(days=days)
        deleted = []
        
        for log_file in self.log_dir.glob("*.log.*"):  # Rotated logs
            if log_file.suffix == '.gz':
                # Try to extract date from filename
                try:
                    # Pattern: something.log.YYYY-MM-DD or similar
                    parts = log_file.name.split('.')
                    for part in parts:
                        try:
                            file_date = datetime.strptime(part, '%Y-%m-%d')
                            if file_date < cutoff:
                                log_file.unlink()
                                deleted.append(str(log_file))
                                break
                        except ValueError:
                            continue
                except Exception as e:
                    logger.warning(f"Could not determine age of {log_file}: {str(e)}")
        
        return len(deleted), deleted


# Global analyzer instance
_log_analyzer: Optional[LogAnalyzer] = None


def get_log_analyzer(log_dir: str = "logs") -> LogAnalyzer:
    """Get or create global log analyzer"""
    global _log_analyzer
    
    if _log_analyzer is None:
        _log_analyzer = LogAnalyzer(log_dir)
    
    return _log_analyzer