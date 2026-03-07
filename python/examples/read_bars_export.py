from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read a bars dataset export manifest and print a preview.")
    parser.add_argument("manifest", type=Path, help="Path to manifest.json written by a bars dataset export job")
    parser.add_argument("--rows", type=int, default=10, help="Number of rows to print")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = json.loads(args.manifest.read_text())
    export_kind = manifest.get("export_kind")
    if export_kind not in {"bars", "ticks"}:
        raise SystemExit(f"Unsupported export kind: {manifest.get('export_kind')}")

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise SystemExit("pyarrow is required to read Parquet exports: pip install pyarrow") from exc

    files = manifest.get("files") or []
    if not files:
        raise SystemExit("Manifest does not contain any files")

    parquet_path = Path(files[0]["path"])
    table = pq.read_table(parquet_path)
    print(
        json.dumps(
            {
                "manifest": str(args.manifest),
                "export_kind": export_kind,
                "rows": table.num_rows,
                "columns": table.column_names,
            },
            indent=2,
        )
    )

    preview = table.slice(0, max(args.rows, 0)).to_pylist()
    print(json.dumps(preview, indent=2, default=str))


if __name__ == "__main__":
    main()
