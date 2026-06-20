#!/usr/bin/env python3
"""Parse Morgan Hill Matrix + UNIT PLANS PDF -> mh-matrix-drawings.json + per-page PDF extracts."""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import openpyxl
import fitz
from pypdf import PdfReader, PdfWriter

ROOT = Path(__file__).resolve().parent.parent
BOX = Path(
    "/Users/geoffreyjackson/Library/CloudStorage/Box-Box/Team Folder/Projects/25048 - Carrollton, TX - Morgan Hill/PROJECT MANAGING/SHOP DRAWINGS"
)
PDF = BOX / "DRAWING SET" / "UNIT PLANS_5.2025.pdf"
XLSX = BOX / "Morgan Hill Matrix NEW.xlsx"
OUT_JSON = ROOT / ".firecrawl" / "mh-matrix-drawings.json"
OUT_PAGES = ROOT / ".firecrawl" / "mh-unit-plan-pages"
OUT_CROPS = ROOT / ".firecrawl" / "mh-unit-plan-crops"
CABINET_GAP = {"MW04.5", "MW05", "MW06"}

UNIT_PAT = re.compile(
    r"UNIT\s+([AB]\d+\.\d+[A-Z])(?:\s+ANSI(?:\s*TYPE)?\s*A|\s+ANSIA)?\s+(?:FLOOR\s+)?PLAN",
    re.I,
)
TITLE_PAT = re.compile(
    r"(?:BUILDING\s+[\d\s&]+\s*-\s*)?UNIT\s+([AB]\d+\.\d+[A-Z])(?:\s+ANSI(?:\s*TYPE)?\s*A|\s+ANSIA)?\s+(?:FLOOR\s+)?(?:ANSI\s+TYPE\s+A\s+)?PLAN",
    re.I,
)


def matrix_name(base: str, span: str) -> str:
    if re.search(r"ANSIA", span, re.I):
        return f"{base} ANSIA"
    if re.search(r"ANSI", span, re.I):
        return f"{base} ANSI A"
    return base


def mode(lst: list[str]) -> str | None:
    return Counter(lst).most_common(1)[0][0] if lst else None


def canonical_kitchen(codes: list[str]) -> tuple[str | None, str | None]:
    if not codes:
        return None, None
    for raw, _ in Counter(codes).most_common():
        mw = mw_base(raw)
        if mw and mw not in CABINET_GAP:
            return raw, mw
    raw = Counter(codes).most_common(1)[0][0]
    return raw, mw_base(raw)


def mw_base(s: str | None) -> str | None:
    if not s:
        return None
    m = re.match(r"(MW[\d.]+)", s)
    return m.group(1) if m else None


def parse_vanity(s: str | None) -> dict | None:
    if not s:
        return None
    s = s.strip()
    m = re.match(r"(\d+)/([A-Z]\d+)", s)
    if m:
        return {"detail": m.group(1), "sheet": m.group(2), "drawing_no": m.group(2), "label": s}
    return {"label": s}


def map_pdf_pages() -> dict[str, int]:
    reader = PdfReader(str(PDF))
    page_of: dict[str, int] = {}
    for i, page in enumerate(reader.pages, start=1):
        text = page.extract_text() or ""
        for m in UNIT_PAT.finditer(text):
            base = m.group(1)
            span = text[max(0, m.start() - 5) : m.end() + 30]
            page_of[matrix_name(base, span)] = i
    if "A3.1C ANSI A" in page_of:
        page_of.setdefault("A3.1C ANSIA", page_of["A3.1C ANSI A"])
    return page_of


def parse_matrix() -> tuple[set[str], dict]:
    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    ws = wb["MASTER"]
    matrix_types: set[str] = set()
    by_kc: dict[str, list[str]] = defaultdict(list)
    by_v1: dict[str, list[str]] = defaultdict(list)
    by_v2: dict[str, list[str]] = defaultdict(list)
    for row in ws.iter_rows(min_row=4, values_only=True):
        if not row[0]:
            continue
        ut = str(row[5]).strip() if row[5] else None
        if not ut:
            continue
        matrix_types.add(ut)
        if row[7]:
            by_kc[ut].append(str(row[7]).strip())
        if row[15]:
            by_v1[ut].append(str(row[15]).strip())
        if row[16]:
            by_v2[ut].append(str(row[16]).strip())
    return matrix_types, {"kc": by_kc, "v1": by_v1, "v2": by_v2}


def _median(vals: list[float], default: float) -> float:
    if not vals:
        return default
    s = sorted(vals)
    return s[len(s) // 2]


def crop_layouts(page_of: dict[str, int]) -> dict[str, str]:
    """Crop each unit-type floor plan from the UNIT PLANS sheet grid (one PDF per type)."""
    OUT_CROPS.mkdir(parents=True, exist_ok=True)
    doc = fitz.open(str(PDF))
    crop_paths: dict[str, str] = {}

    by_page: dict[int, list[str]] = defaultdict(list)
    for ut, pg in page_of.items():
        by_page[pg].append(ut)

    for page_num, types_on_page in by_page.items():
        page = doc[page_num - 1]
        page_rect = page.rect
        hits: list[tuple[str, fitz.Rect, str]] = []
        for block in page.get_text("dict")["blocks"]:
            if block.get("type") != 0:
                continue
            for line in block["lines"]:
                txt = "".join(span["text"] for span in line["spans"]).strip()
                m = TITLE_PAT.search(txt)
                if not m:
                    continue
                name = matrix_name(m.group(1), txt)
                hits.append((name, fitz.Rect(line["bbox"]), txt))

        if not hits:
            continue

        hits.sort(key=lambda h: (h[1].y0, h[1].x0))
        rows: list[list[tuple[str, fitz.Rect, str]]] = []
        for name, rect, txt in hits:
            for row in rows:
                if abs(rect.y0 - row[0][1].y0) < 22:
                    row.append((name, rect, txt))
                    break
            else:
                rows.append([(name, rect, txt)])
        for row in rows:
            row.sort(key=lambda r: r[1].x0)

        col_widths: list[float] = []
        row_heights: list[float] = []
        for row in rows:
            for i in range(len(row) - 1):
                col_widths.append(row[i + 1][1].x0 - row[i][1].x0)
        for i in range(len(rows) - 1):
            row_heights.append(rows[i + 1][0][1].y0 - rows[i][0][1].y0)

        col_w = _median(col_widths, page_rect.width * 0.24)
        row_h = _median(row_heights, 95.0)
        margin = 6.0

        for ri, row in enumerate(rows):
            for ci, (name, rect, _txt) in enumerate(row):
                if name not in types_on_page:
                    continue
                x0 = max(page_rect.x0, rect.x0 - margin)
                if ci + 1 < len(row):
                    x1 = min(page_rect.x1, row[ci + 1][1].x0 - margin)
                else:
                    x1 = min(page_rect.x1, rect.x0 + col_w)
                y0 = max(page_rect.y0, rect.y0 - margin)
                if ri + 1 < len(rows):
                    y1 = min(page_rect.y1, rows[ri + 1][0][1].y0 - margin)
                else:
                    y1 = min(page_rect.y1, rect.y0 + row_h)
                clip = fitz.Rect(x0, y0, x1, y1)
                safe = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_")
                out_path = OUT_CROPS / f"{safe}.pdf"
                cropped = fitz.open()
                new_page = cropped.new_page(width=clip.width, height=clip.height)
                new_page.show_pdf_page(new_page.rect, doc, page_num - 1, clip=clip)
                cropped.save(str(out_path))
                cropped.close()
                crop_paths[name] = str(out_path)

        # Single-plan pages: if only one type expected but title match failed naming, use full lower sheet
        if len(types_on_page) == 1 and types_on_page[0] not in crop_paths:
            ut = types_on_page[0]
            clip = fitz.Rect(page_rect.x0 + 20, page_rect.y0 + 80, page_rect.x1 - 20, page_rect.y1 - 40)
            safe = re.sub(r"[^A-Za-z0-9._-]+", "_", ut).strip("_")
            out_path = OUT_CROPS / f"{safe}.pdf"
            cropped = fitz.open()
            new_page = cropped.new_page(width=clip.width, height=clip.height)
            new_page.show_pdf_page(new_page.rect, doc, page_num - 1, clip=clip)
            cropped.save(str(out_path))
            cropped.close()
            crop_paths[ut] = str(out_path)

    doc.close()

    for alias, canonical in {"A3.1C ANSIA": "A3.1C ANSI A"}.items():
        if canonical in crop_paths:
            crop_paths[alias] = crop_paths[canonical]

    return crop_paths


def extract_pages(pages_needed: set[int]) -> dict[int, str]:
    OUT_PAGES.mkdir(parents=True, exist_ok=True)
    reader = PdfReader(str(PDF))
    paths: dict[int, str] = {}
    for n in sorted(pages_needed):
        writer = PdfWriter()
        writer.add_page(reader.pages[n - 1])
        out = OUT_PAGES / f"page_{n:02d}.pdf"
        with out.open("wb") as f:
            writer.write(f)
        paths[n] = str(out)
    return paths


def main() -> None:
    page_of = map_pdf_pages()
    matrix_types, cols = parse_matrix()
    pages_needed = {page_of[t] for t in matrix_types if t in page_of}
    page_paths = extract_pages(pages_needed)
    crop_paths = crop_layouts(page_of)

    out: dict = {
        "source_pdf": str(PDF),
        "page_paths": {str(k): v for k, v in page_paths.items()},
        "crop_paths": crop_paths,
        "types": {},
        "gaps": {"layout": [], "kitchen_pdf": []},
    }

    for ut in sorted(matrix_types):
        kc_raw, mw = canonical_kitchen(cols["kc"][ut])
        v1 = parse_vanity(mode(cols["v1"][ut]) if cols["v1"][ut] else None)
        v2 = parse_vanity(mode(cols["v2"][ut]) if cols["v2"][ut] else None)
        page = page_of.get(ut)
        crop = crop_paths.get(ut)
        out["types"][ut] = {
            "layout_page": page,
            "layout_page_pdf": page_paths.get(page) if page else None,
            "layout_crop_pdf": crop,
            "kitchen_cab_raw": kc_raw,
            "kitchen_drawing_no": mw,
            "vanity_1": v1,
            "vanity_2": v2,
        }
        if not page:
            out["gaps"]["layout"].append(ut)
        if mw in CABINET_GAP:
            out["gaps"]["kitchen_pdf"].append(ut)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, indent=2))
    print(
        f"Wrote {OUT_JSON} — {len(out['types'])} types, {len(pages_needed)} pages, "
        f"{len(crop_paths)} crops, layout gaps: {len(out['gaps']['layout'])}"
    )


if __name__ == "__main__":
    main()
