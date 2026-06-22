"""
Reporting module for generating HTML reports and summaries
"""

from .generator import ReportGenerator, get_report_generator

__all__ = [
    'ReportGenerator',
    'get_report_generator'
]