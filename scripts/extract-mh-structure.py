#!/usr/bin/env python3
"""Parse Morgan Hill Matrix MASTER -> .firecrawl/mh-structure.json"""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
XLSX = Path(
    "/Users/geoffreyjackson/Library/CloudStorage/Box-Box/Team Folder/Projects/"
    "25048 - Carrollton, TX - Morgan Hill/PROJECT MANAGING/SHOP DRAWINGS/"
    "Morgan Hill Matrix NEW.xlsx"
)
OUT = ROOT / ".firecrawl" / "mh-structure.json"


def scheme_to_color_code(raw: str | None) -> str | None:
    if not raw:
        return None
    s = str(raw).strip().lower()
    if s in ("scheme 1", "scheme1", "sch1"):
        return "scheme1"
    if s in ("scheme 2", "scheme2", "sch2"):
        return "scheme2"
    return None


def color_from_kitchen_cab(kc: str | None) -> str | None:
    if not kc:
        return None
    m = re.search(r"_SCH(\d+)", str(kc).upper())
    if m:
        return f"scheme{m.group(1)}"
    return None


def main() -> None:
    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    ws = wb["MASTER"]

    units: list[list] = []
    type_vanity: dict[str, list[tuple[str | None, str | None]]] = defaultdict(list)
    inconsistent: dict[str, list[int]] = {}

    for row in ws.iter_rows(min_row=4, values_only=True):
        if not row[0]:
            continue
        unit_number = str(int(row[0])) if isinstance(row[0], (int, float)) else str(row[0]).strip()
        area = str(row[2]).strip() if row[2] else None
        truck = int(row[3]) if row[3] is not None else None
        phase = int(row[4]) if row[4] is not None else None
        utype = str(row[5]).strip() if row[5] else None
        scheme_col = str(row[8]).strip() if row[8] else None
        kc = str(row[7]).strip() if row[7] else None
        v1 = str(row[15]).strip() if row[15] else None
        v2 = str(row[16]).strip() if row[16] else None

        color_code = scheme_to_color_code(scheme_col) or color_from_kitchen_cab(kc)
        units.append([unit_number, area, truck, phase, color_code])

        if utype:
            type_vanity[utype].append((v1, v2))

    type_bath: dict[str, int] = {}
    for name, pairs in type_vanity.items():
        counts = Counter()
        for v1, v2 in pairs:
            n = (1 if v1 else 0) + (1 if v2 else 0)
            counts[n] += 1
        bath = counts.most_common(1)[0][0]
        type_bath[name] = bath
        if len(counts) > 1:
            inconsistent[name] = sorted(counts.keys())

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(
        json.dumps(
            {
                "source": str(XLSX),
                "units": units,
                "type_bath": type_bath,
                "inconsistent": inconsistent,
            },
            indent=2,
        )
    )
    with_color = sum(1 for u in units if u[4])
    print(f"Wrote {OUT} — {len(units)} units, color_code on {with_color}, {len(type_bath)} types")


if __name__ == "__main__":
    main()
