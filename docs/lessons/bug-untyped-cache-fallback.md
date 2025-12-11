# Fallback match list lost its DirEntry type

## What went wrong
When a file source was not present in `scan_cache`, `build_symlinks` initialized `matches = []` without a type. After the switch to `os.DirEntry` objects, that fallback produced `list[Any]`, so assigning cached DirEntry objects into it triggered strict mypy failures.

## Root cause (git history)
The same `93fcebe` refactor that switched from glob strings to `os.DirEntry` values updated the happy-path loop but did not update the fallback branch. The untyped empty list only mattered once strict mypy consumed the new DirEntry values, surfacing the bug in the later CI run.

## Fix
Annotate the fallback with `list[os.DirEntry[str]]` so both cached and freshly scanned results share the same type. That keeps the new DirEntry-based scan data consistent and restores mypy compatibility.
