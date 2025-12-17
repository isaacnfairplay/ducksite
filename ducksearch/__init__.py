"""Public Ducksearch API surface.

This module re-exports the primary entry points so users can rely on stable,
importable symbols instead of reaching into private modules.
"""

from ducksearch.loader import CACHE_SUBDIRS, RootLayout, validate_root
from ducksearch.report_parser import Parameter, ParameterType, Report, parse_report_sql
from ducksearch.runtime import ExecutionError, ExecutionResult, execute_report

__all__ = [
    "CACHE_SUBDIRS",
    "ExecutionError",
    "ExecutionResult",
    "Parameter",
    "ParameterType",
    "Report",
    "execute_report",
    "parse_report_sql",
    "RootLayout",
    "validate_root",
]
