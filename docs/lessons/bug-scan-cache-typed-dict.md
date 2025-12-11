# Scan cache still used `object` payloads

## What went wrong
Strict mypy runs started failing because `build_symlinks` pulled `pattern`, `matches`, and `error` out of `scan_cache` as `object` values. Downstream assignments then conflicted with the expected `str`, `list[os.DirEntry]`, and `bool` types, breaking all strict mypy tests for modules that import `symlinks`.

## Root cause (git history)
Commit `93fcebe` ("Use scandir entries for upstream fingerprints") replaced `glob.glob` strings with `os.DirEntry` results in `_collect_upstream_matches`, but its return type stayed `dict[str, object]`. That meant the new DirEntry payloads were stored as opaque objects, so later refactors (e.g., in `builder`, `forms`, `fast_server`) inherited the wrong types and mypy could no longer prove safety.

## Fix
Introduce a `ScanResult` `TypedDict` and a `ScanCache` alias to give `_collect_upstream_matches` a typed return value. `build_symlinks` now reads strongly typed fields from the cache, restoring correct types for the rest of the module and satisfying strict mypy.
