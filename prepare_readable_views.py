#!/usr/bin/env python3
"""Create text-readable views and previews from the supplied Binance ZIP archives."""
from __future__ import annotations
import csv
import gzip
import io
import os
import zipfile
from collections import defaultdict, deque
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "readable_data"
REPO = "Viralchemist/binance-futures-historical-data-2024-10-to-2026-10"
RAW = f"https://raw.githubusercontent.com/{REPO}/main/readable_data"
ARCHIVES = {
    "1h": ROOT / "BINANCE_FUTURES_1h_2024-10_to_2026-10.zip",
    "1m": ROOT / "BINANCE_FUTURES_1m_2024-10_to_2026-10.zip",
    "5m": ROOT / "BINANCE_FUTURES_5m_2024-10_to_2026-10.zip",
    "funding": ROOT / "BINANCE_FUTURES_FUNDING_2024-10_to_2026-10.zip",
    "audit": ROOT / "BINANCE_FUTURES_AUDIT_REPORTS.zip",
}

for sub in ["1h_by_symbol", "funding_by_symbol", "previews_1m_5m", "audit_reports"]:
    (OUT / sub).mkdir(parents=True, exist_ok=True)

manifest = []

def csv_reader_from_zip(zf: zipfile.ZipFile, member: str):
    raw = zf.open(member)
    gz = gzip.GzipFile(fileobj=raw, mode="rb")
    text = io.TextIOWrapper(gz, encoding="utf-8-sig", newline="")
    return raw, gz, text, csv.reader(text)

def grouped_members(zf: zipfile.ZipFile, prefix: str):
    groups = defaultdict(list)
    for member in zf.namelist():
        if not member.endswith(".csv.gz") or not member.startswith(prefix):
            continue
        parts = member.split("/")
        symbol = parts[-2]
        groups[symbol].append(member)
    for values in groups.values():
        values.sort()
    return groups

def file_bytes(path: Path) -> int:
    return path.stat().st_size

# Full text-readable 1h CSVs, one per symbol to keep each file reasonably sized.
with zipfile.ZipFile(ARCHIVES["1h"]) as zf:
    groups = grouped_members(zf, "market_data/klines/1h/")
    for symbol, members in sorted(groups.items()):
        out_path = OUT / "1h_by_symbol" / f"{symbol}_1h.csv"
        rows = 0
        columns = None
        with out_path.open("w", encoding="utf-8", newline="") as out_f:
            writer = csv.writer(out_f)
            for member in members:
                raw, gz, text, reader = csv_reader_from_zip(zf, member)
                try:
                    header = next(reader)
                    if columns is None:
                        columns = header
                        writer.writerow(["symbol"] + columns)
                    elif header != columns:
                        raise ValueError(f"Inconsistent 1h columns in {member}")
                    for row in reader:
                        if not row or row == columns:
                            continue
                        writer.writerow([symbol] + row)
                        rows += 1
                finally:
                    text.close()
                    gz.close()
                    raw.close()
        manifest.append({"dataset": "1h", "symbol": symbol, "path": str(out_path.relative_to(ROOT)), "rows": rows, "bytes": file_bytes(out_path), "source": ARCHIVES["1h"].name})

# Full funding CSVs, one per symbol.
with zipfile.ZipFile(ARCHIVES["funding"]) as zf:
    groups = grouped_members(zf, "market_data/funding/")
    for symbol, members in sorted(groups.items()):
        out_path = OUT / "funding_by_symbol" / f"{symbol}_funding.csv"
        rows = 0
        columns = None
        with out_path.open("w", encoding="utf-8", newline="") as out_f:
            writer = csv.writer(out_f)
            for member in members:
                raw, gz, text, reader = csv_reader_from_zip(zf, member)
                try:
                    header = next(reader)
                    if columns is None:
                        columns = header
                        writer.writerow(columns)
                    elif header != columns:
                        raise ValueError(f"Inconsistent funding columns in {member}")
                    for row in reader:
                        if not row or row == columns:
                            continue
                        writer.writerow(row)
                        rows += 1
                finally:
                    text.close()
                    gz.close()
                    raw.close()
        manifest.append({"dataset": "funding", "symbol": symbol, "path": str(out_path.relative_to(ROOT)), "rows": rows, "bytes": file_bytes(out_path), "source": ARCHIVES["funding"].name})

# Text previews for 1m/5m: first 20 and last 20 rows for each symbol.
for timeframe in ["1m", "5m"]:
    with zipfile.ZipFile(ARCHIVES[timeframe]) as zf:
        groups = grouped_members(zf, f"market_data/klines/{timeframe}/")
        for symbol, members in sorted(groups.items()):
            first_rows = []
            last_rows = deque(maxlen=20)
            header = None
            # Keep the first 20 observations from the earliest member.
            raw, gz, text, reader = csv_reader_from_zip(zf, members[0])
            try:
                header = next(reader)
                for row in reader:
                    if row and row != header:
                        first_rows.append(row)
                        if len(first_rows) == 20:
                            break
            finally:
                text.close(); gz.close(); raw.close()
            # Keep the last 20 observations from the latest member.
            raw, gz, text, reader = csv_reader_from_zip(zf, members[-1])
            try:
                latest_header = next(reader)
                if header != latest_header:
                    raise ValueError(f"Inconsistent {timeframe} columns for {symbol}")
                for row in reader:
                    if row and row != header:
                        last_rows.append(row)
            finally:
                text.close(); gz.close(); raw.close()
            sample = first_rows + list(last_rows)
            # Avoid duplicate rows if the archive covers only one monthly member.
            seen = set()
            unique = []
            for row in sample:
                key = row[0] if row else ""
                if key not in seen:
                    seen.add(key)
                    unique.append(row)
            out_path = OUT / "previews_1m_5m" / f"{symbol}_{timeframe}_preview.csv"
            with out_path.open("w", encoding="utf-8", newline="") as out_f:
                writer = csv.writer(out_f)
                writer.writerow(["symbol"] + header)
                writer.writerows([[symbol] + row for row in unique])
            manifest.append({"dataset": f"{timeframe}_preview", "symbol": symbol, "path": str(out_path.relative_to(ROOT)), "rows": len(unique), "bytes": file_bytes(out_path), "source": ARCHIVES[timeframe].name})

# Extract the audit archive: its files are already text/JSON and small enough to read directly.
with zipfile.ZipFile(ARCHIVES["audit"]) as zf:
    for member in zf.namelist():
        if member.endswith("/"):
            continue
        base = Path(member).name
        out_path = OUT / "audit_reports" / base
        out_path.write_bytes(zf.read(member))
        manifest.append({"dataset": "audit", "symbol": "", "path": str(out_path.relative_to(ROOT)), "rows": "", "bytes": file_bytes(out_path), "source": ARCHIVES["audit"].name})

# Dataset manifest.
manifest_path = OUT / "manifest.csv"
with manifest_path.open("w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["dataset", "symbol", "path", "rows", "bytes", "source"])
    writer.writeheader()
    writer.writerows(manifest)

# A self-contained index with direct Raw links for agents.
lines = [
    "# Text-readable dataset files",
    "",
    "These are plain-text CSV/JSON/Markdown files extracted from the source ZIPs. The original ZIP archives remain in the repository. This folder is intended for agents/tools that cannot unzip binaries.",
    "",
    "## Full hourly data by symbol",
    "",
]
for row in manifest:
    if row["dataset"] == "1h":
        rel = row["path"].replace("\\", "/").removeprefix("readable_data/")
        lines.append(f'- [{row["symbol"]} hourly CSV]({RAW}/{rel}) — {row["rows"]} rows')
lines += ["", "## Full funding data by symbol", ""]
for row in manifest:
    if row["dataset"] == "funding":
        rel = row["path"].replace("\\", "/").removeprefix("readable_data/")
        lines.append(f'- [{row["symbol"]} funding CSV]({RAW}/{rel}) — {row["rows"]} rows')
lines += ["", "## 1m/5m text previews", "", "Each preview contains up to 20 opening rows and 20 ending rows for that symbol/timeframe. Full 1m and 5m data remain in the original archives; these very large datasets are not duplicated as multi-gigabyte plain-text files.", ""]
for row in manifest:
    if row["dataset"].endswith("_preview"):
        rel = row["path"].replace("\\", "/").removeprefix("readable_data/")
        lines.append(f'- [{row["symbol"]} {row["dataset"]} CSV]({RAW}/{rel}) — {row["rows"]} sample rows')
lines += ["", "## Audit reports", ""]
for row in manifest:
    if row["dataset"] == "audit":
        rel = row["path"].replace("\\", "/").removeprefix("readable_data/")
        lines.append(f'- [{Path(row["path"]).name}]({RAW}/{rel})')
lines += ["", "## Structure and checks", "", f'- [Machine-readable manifest]({RAW}/manifest.csv)', "- The original kline column order is preserved, with a leading `symbol` column added to each hourly CSV and preview.", "- Funding CSV column names and rows are preserved from the source files.", "- Source archives are listed in the parent repository README.", ""]
(OUT / "README.md").write_text("\n".join(lines), encoding="utf-8")
print(f"Created {len(manifest)} readable files/records under {OUT}")
print(f"Total bytes: {sum(p.stat().st_size for p in OUT.rglob('*') if p.is_file()):,}")
print(f"Hourly symbol files: {sum(1 for r in manifest if r['dataset']=='1h')}; funding: {sum(1 for r in manifest if r['dataset']=='funding')}; previews: {sum(1 for r in manifest if r['dataset'].endswith('_preview'))}; audit: {sum(1 for r in manifest if r['dataset']=='audit')}")
