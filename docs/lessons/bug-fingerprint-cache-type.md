# Fingerprint cache used outdated types

## What went wrong
`_file_source_fingerprints` still accepted `dict[int, dict[str, object]]`, so `cached.get("matches")` produced `object` instead of `list[os.DirEntry]`. That left the module with mismatched types between the scanning and fingerprinting code paths, causing strict mypy failures and risking runtime surprises if non-list values appeared.

## Root cause (git history)
Commit `93fcebe` updated `_collect_upstream_matches` to return DirEntry objects but did not propagate the new type information to `_file_source_fingerprints`. The function signature and local variable types stayed aligned with the pre-DirEntry design, leaving the mismatch to be caught only when mypy was rerun.

## Fix
Re-type `_file_source_fingerprints` to consume the new `ScanCache` structure so cached matches are treated as `list[os.DirEntry[str]]`. This keeps the fingerprint computation consistent with the updated scanning logic and clears the strict mypy errors.
