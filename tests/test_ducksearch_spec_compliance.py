from ducksearch.loader import CACHE_SUBDIRS
from ducksearch.report_parser import SUPPORTED_BLOCKS, PLACEHOLDER_RE


def test_supported_blocks_cover_spec_list():
    expected = {
        "PARAMS",
        "CONFIG",
        "SOURCES",
        "CACHE",
        "TABLE",
        "SEARCH",
        "FACETS",
        "CHARTS",
        "DERIVED_PARAMS",
        "LITERAL_SOURCES",
        "BINDINGS",
        "IMPORTS",
        "SECRETS",
    }
    assert set(SUPPORTED_BLOCKS) == expected


def test_cache_subdirs_match_spec_layout():
    expected = {
        "artifacts",
        "slices",
        "materialize",
        "literal_sources",
        "bindings",
        "facets",
        "charts",
        "manifests",
        "tmp",
    }
    assert set(CACHE_SUBDIRS) == expected


def test_placeholder_regex_covers_allowed_types():
    allowed = {"config", "param", "bind", "mat", "import", "ident", "path"}
    # Pull placeholder types from the regex by matching each allowed key.
    for key in allowed:
        assert PLACEHOLDER_RE.search(f"{{{{ {key} foo }}}}")
