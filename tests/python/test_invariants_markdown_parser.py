import json
from pathlib import Path

from ducksite.markdown_parser import PageQueries, parse_markdown_page, build_page_config


def test_pagequeries_types_and_structures(tmp_path: Path) -> None:
    md = tmp_path / "page.md"
    md.write_text(
        """
# Title

```sql q1
SELECT 1 AS x;
```

```echart v1
data_query: q1
type: bar
x: x
y: x
```

```table t1
query: q1
```

""",
        encoding="utf-8",
    )
    pq = parse_markdown_page(md, Path("page.md"))
    cfg = json.loads(build_page_config(pq))

    assert "v1" in cfg["visualizations"]
    assert cfg["visualizations"]["v1"]["data_query"] == "q1"

    assert "tables" in cfg
    assert cfg["tables"]["t1"]["query"] == "q1"
