"""Shared string enums for core ducksite resources."""
from enum import StrEnum


class AssetPath(StrEnum):
    INDEX = "/index.html"
    CONTRACT_JS = "/js/ducksite_contract.js"
    ECHARTS_JS = "/js/echarts.min.js"
    DUCKDB_BUNDLE_JS = "/js/duckdb-bundle.js"
    DEMO_DATA = "/data/demo"


class Scheme(StrEnum):
    HTTP = "http"
    HTTPS = "https"
