#!/usr/bin/env python3
#!/usr/bin/env python3
"""
Log management and analysis CLI tool
"""
import argparse
import logging
import sys
import os
import json
from datetime import datetime, timedelta
from pathlib import Path

#CRITICAL :
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from logging.structured_logger import get_structured_logger, setup_logging
from logging.log_analyzer import get_log_analyzer

# Set up logging for the tool itself
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LogCLI:
    """Command-line interface for log management"""
    
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.analyzer = get_log_analyzer(str(log_dir))
    
    def show_recent(self, 
                   count: int = 20,
                   level: str = None,
                   json_output: bool = False) -> None:
        """Show recent log entries"""
        hours = max(24, count // 100)  # Dynamically determine time window
        
        logs = self.analyzer.read_logs(
            since=datetime.utcnow() - timedelta(hours=hours),
            level=level
        )[-count:]  # Get last N entries
        
        if not logs:
            print("No logs found")
            return
        
        if json_output:
            print(json.dumps(logs, indent=2, default=str))
        else:
            print("\n" + "=" * 100)
            print("RECENT LOG ENTRIES")
            print("=" * 100)
            
            for log in logs:
                self._print_log_entry(log)
    
    def show_errors(self, hours: int = 24, json_output: bool = False) -> None:
        """Show error logs"""
        analysis = self.analyzer.analyze_errors(hours=hours)
        
        if json_output:
            print(json.dumps(analysis, indent=2, default=str))
        else:
            print("\n" + "=" * 100)
            print(f"ERROR ANALYSIS (Last {hours} hours)")
            print("=" * 100)
            
            print(f"\nTotal Errors: {analysis['total_errors']}")
            
            if analysis['most_common_errors']:
                print("\nMost Common Errors:")
                for error, count in analysis['most_common_errors']:
                    print(f"  {count:4d} x {error}")
            
            if analysis['recent_errors']:
                print(f"\nRecent Errors (last 10):")
                for error in analysis['recent_errors']:
                    self._print_log_entry(error, show_context=False)
    
    def performance(self, hours: int = 1, json_output: bool = False) -> None:
        """Show performance metrics"""
        report = self.analyzer.performance_report(hours=hours)
        
        if json_output:
            print(json.dumps(report, indent=2, default=str))
        else:
            print("\n" + "=" * 100)
            print(f"PERFORMANCE REPORT (Last {hours} hours)")
            print("=" * 100)
            
            print(f"\nTotal Timed Operations: {report['total_timed_operations']}")
            
            if report['operations']:
                print("\nOperation Performance:")
                for op, stats in report['operations'].items():
                    print(f"\n  {op}:")
                    print(f"    Count:    {stats['count']}")
                    print(f"    Average:  {stats['avg_ms']:.1f} ms")
                    print(f"    Min:      {stats['min_ms']:.1f} ms")
                    print(f"    Max:      {stats['max_ms']:.1f} ms")
                    print(f"    95th %:   {stats['p95_ms']:.1f} ms")
            
            if report['slowest_operations']:
                print("\nSlowest Operations (by average):")
                for op, stats in report['slowest_operations']:
                    print(f"  {op}: {stats['avg_ms']:.1f} ms avg")
    
    def summary(self, hours: int = 1, json_output: bool = False) -> None:
        """Show log summary"""
        summary = self.analyzer.log_summary(hours=hours)
        
        if json_output:
            print(json.dumps(summary, indent=2, default=str))
        else:
            print("\n" + "=" * 100)
            print(f"LOG SUMMARY (Last {hours} hours)")
            print("=" * 100)
            
            print(f"\nTotal Logs: {summary['total_logs']}")
            
            if 'level_distribution' in summary:
                print("\nLevel Distribution:")
                for level, count in summary['level_distribution'].items():
                    print(f"  {level:8}: {count}")
            
            if 'unique_loggers' in summary:
                print(f"\nUnique Loggers: {len(summary['unique_loggers'])}")
                if len(summary['unique_loggers']) <= 10:
                    for logger_name in sorted(summary['unique_loggers']):
                        print(f"  - {logger_name}")
            
            if 'recent_messages' in summary and summary['recent_messages']:
                print("\nRecent Messages:")
                for msg in summary['recent_messages']:
                    if msg:
                        print(f"  - {msg}")
    
    def search(self, query: str, hours: int = 24, json_output: bool = False) -> None:
        """Search logs"""
        results = self.analyzer.search_logs(query, hours=hours)
        
        if json_output:
            print(json.dumps(results, indent=2, default=str))
        else:
            print(f"\nFound {len(results)} matching log entries for '{query}':")
            print("=" * 100)
            
            for log in results[:50]:  # Limit output
                self._print_log_entry(log)
            
            if len(results) > 50:
                print(f"\n... and {len(results) - 50} more entries")
    
    def export(self, output_file: str, hours: int = 24, format: str = 'json') -> None:
        """Export logs to file"""
        self.analyzer.export_logs(output_file, hours=hours, format=format)
        print(f"Exported logs to {output_file}")
    
    def cleanup(self, days: int = 30, dry_run: bool = False) -> None:
        """Clean up old log files"""
        if dry_run:
            print(f"Dry run: Would clean up logs older than {days} days")
            # Create a test analyzer with the same directory
            analyzer = get_log_analyzer(str(self.log_dir))
            
            # List files that would be deleted
            for log_file in self.log_dir.glob("*.log.*"):
                print(f"  Would consider: {log_file.name}")
        else:
            deleted_count, deleted_files = self.analyzer.cleanup_old_logs(days)
            print(f"Deleted {deleted_count} old log files:")
            for file in deleted_files:
                print(f"  - {file}")
    
    def test_logging(self, count: int = 5) -> None:
        """Test logging by generating test log entries"""
        test_logger = get_structured_logger()
        
        print(f"Generating {count} test log entries...")
        
        # Set some context
        test_logger.set_context(
            request_id="test-123",
            component="log_tool",
            operation="test_logging"
        )
        
        # Generate test logs
        for i in range(count):
            test_logger.debug(f"Test debug message {i}", iteration=i)
            test_logger.info(f"Test info message {i}", iteration=i)
            test_logger.warning(f"Test warning message {i}", iteration=i)
        
        # Test error logging
        try:
            raise ValueError("Test exception for logging")
        except ValueError as e:
            test_logger.error(f"Test error message", exc_info=e)
        
        # Test timing
        with test_logger.timer("test_operation"):
            import time
            time.sleep(0.1)
        
        print(f"Generated {count * 3 + 2} test log entries")
        print(f"Logs written to: {self.log_dir}/")
    
    def _print_log_entry(self, log: dict, show_context: bool = True) -> None:
        """Print a single log entry in readable format"""
        timestamp = log.get('timestamp', '')
        level = log.get('level', 'UNKNOWN')
        message = log.get('message', '')
        logger_name = log.get('logger', '')
        module = log.get('module', '')
        function = log.get('function', '')
        
        # Color codes for levels
        colors = {
            'DEBUG': '\033[90m',    # Gray
            'INFO': '\033[92m',     # Green
            'WARNING': '\033[93m',  # Yellow
            'ERROR': '\033[91m',    # Red
            'CRITICAL': '\033[41m'  # Red background
        }
        reset = '\033[0m'
        
        color = colors.get(level, '')
        
        print(f"\n{color}{timestamp} {level:8} [{logger_name}]{reset}")
        print(f"  {message}")
        
        if module and function:
            print(f"  📍 {module}.{function}()")
        
        if 'duration_ms' in log:
            print(f"  ⏱️  Duration: {log['duration_ms']} ms")
        
        if 'exception' in log:
            exc = log['exception']
            print(f"  💥 {exc.get('type', 'Exception')}: {exc.get('message', '')}")
        
        if show_context and 'context' in log:
            context = log['context']
            if context:
                print(f"  📋 Context: {context}")


def main():
    parser = argparse.ArgumentParser(
        description='Livestock Outbreak Detection - Log Management Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog.py recent                        Show recent logs
  %(prog.py recent --count 50             Show 50 recent logs
  %(prog.py errors                        Show error logs
  %(prog.py errors --hours 48             Show errors from last 48 hours
  %(prog.py performance                   Show performance metrics
  %(prog.py summary                       Show log summary
  %(prog.py search "database"             Search for "database" in logs
  %(prog.py export logs.json              Export logs to JSON
  %(prog.py cleanup --days 7              Clean up logs older than 7 days
  %(prog.py test                          Generate test log entries
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Recent command
    recent_parser = subparsers.add_parser('recent', help='Show recent logs')
    recent_parser.add_argument('--count', type=int, default=20,
                              help='Number of logs to show (default: 20)')
    recent_parser.add_argument('--level', help='Filter by log level')
    recent_parser.add_argument('--json', action='store_true',
                              help='Output in JSON format')
    
    # Errors command
    errors_parser = subparsers.add_parser('errors', help='Show error logs')
    errors_parser.add_argument('--hours', type=int, default=24,
                              help='Hours to look back (default: 24)')
    errors_parser.add_argument('--json', action='store_true',
                              help='Output in JSON format')
    
    # Performance command
    perf_parser = subparsers.add_parser('performance', help='Show performance metrics')
    perf_parser.add_argument('--hours', type=int, default=1,
                            help='Hours to analyze (default: 1)')
    perf_parser.add_argument('--json', action='store_true',
                            help='Output in JSON format')
    
    # Summary command
    summary_parser = subparsers.add_parser('summary', help='Show log summary')
    summary_parser.add_argument('--hours', type=int, default=1,
                               help='Hours to summarize (default: 1)')
    summary_parser.add_argument('--json', action='store_true',
                               help='Output in JSON format')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search logs')
    search_parser.add_argument('query', help='Search query')
    search_parser.add_argument('--hours', type=int, default=24,
                              help='Hours to search (default: 24)')
    search_parser.add_argument('--json', action='store_true',
                              help='Output in JSON format')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export logs')
    export_parser.add_argument('output_file', help='Output file')
    export_parser.add_argument('--hours', type=int, default=24,
                              help='Hours to export (default: 24)')
    export_parser.add_argument('--format', choices=['json', 'csv'], default='json',
                              help='Export format')
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean up old logs')
    cleanup_parser.add_argument('--days', type=int, default=30,
                               help='Delete logs older than N days (default: 30)')
    cleanup_parser.add_argument('--dry-run', action='store_true',
                               help='Show what would be deleted without deleting')
    
    # Test command
    subparsers.add_parser('test', help='Generate test log entries')
    
    # Global arguments
    parser.add_argument('--log-dir', default='logs',
                       help='Log directory (default: logs)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Initialize CLI
    cli = LogCLI(args.log_dir)
    
    # Execute command
    if args.command == 'recent':
        cli.show_recent(args.count, args.level, args.json)
        
    elif args.command == 'errors':
        cli.show_errors(args.hours, args.json)
        
    elif args.command == 'performance':
        cli.performance(args.hours, args.json)
        
    elif args.command == 'summary':
        cli.summary(args.hours, args.json)
        
    elif args.command == 'search':
        cli.search(args.query, args.hours, args.json)
        
    elif args.command == 'export':
        cli.export(args.output_file, args.hours, args.format)
        
    elif args.command == 'cleanup':
        cli.cleanup(args.days, args.dry_run)
        
    elif args.command == 'test':
        cli.test_logging()


if __name__ == "__main__":
    main()