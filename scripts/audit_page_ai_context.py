#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

REQUIRED_DEFAULT = [
    "page_path",
    "page_type",
    "title",
    "summary",
    "manufacturers",
    "represented_businesses",
    "products_or_services",
    "key_talking_points",
    "faq_context",
    "cta_context",
    "bot_instructions",
]

BOT_MARKERS = [
    'id="ai-panel"',
    'id="ai-chat"',
    'id="botPanel"',
    'class="bot-panel"',
    'id="cg-page-bot"',
    '/assets/js/page_ai_context.js',
]


def normalize_page_path(file_path: Path, repo_root: Path) -> str:
    rel = file_path.relative_to(repo_root).as_posix()
    if rel == "index.html":
        return "/"
    if rel.endswith("/index.html"):
        return "/" + rel[: -len("index.html")]
    return "/" + rel


def load_registry(registry_path: Path) -> Dict:
    if not registry_path.exists():
        return {"required_fields": REQUIRED_DEFAULT, "pages": []}
    data = json.loads(registry_path.read_text(encoding="utf-8"))
    if "required_fields" not in data:
        data["required_fields"] = REQUIRED_DEFAULT
    if "pages" not in data:
        data["pages"] = []
    return data


def has_bot(html: str) -> bool:
    lower = html.lower()
    return any(marker.lower() in lower for marker in BOT_MARKERS)


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    registry_path = repo_root / "config" / "page_context_registry.json"
    out_path = repo_root / "runs" / "page_context_audit.md"

    registry = load_registry(registry_path)
    required = list(registry.get("required_fields", REQUIRED_DEFAULT))
    pages = list(registry.get("pages", []))
    context_map = {str(p.get("page_path", "")).strip(): p for p in pages if p.get("page_path")}

    html_files: List[Path] = [
        p for p in repo_root.rglob("*.html")
        if ".git/" not in p.as_posix() and "docs_learning/" not in p.as_posix()
    ]

    bot_pages: List[str] = []
    context_pages: List[str] = []
    missing_context_pages: List[str] = []

    for html_file in sorted(html_files):
        page_path = normalize_page_path(html_file, repo_root)
        html = html_file.read_text(encoding="utf-8", errors="ignore")
        if has_bot(html):
            bot_pages.append(page_path)
        if page_path in context_map:
            context_pages.append(page_path)
        else:
            missing_context_pages.append(page_path)

    missing_required: Dict[str, List[str]] = {}
    for page_path, entry in sorted(context_map.items()):
        missing = [field for field in required if field not in entry]
        if missing:
            missing_required[page_path] = missing

    lines: List[str] = []
    lines.append("# Page AI Context Audit")
    lines.append("")
    lines.append(f"- html_pages_scanned: {len(html_files)}")
    lines.append(f"- pages_with_bot_marker: {len(bot_pages)}")
    lines.append(f"- pages_with_context_entry: {len(context_pages)}")
    lines.append(f"- context_entries_missing_required_fields: {len(missing_required)}")
    lines.append("")

    lines.append("## Pages With Bot")
    if bot_pages:
        for page in bot_pages:
            lines.append(f"- {page}")
    else:
        lines.append("- (none)")
    lines.append("")

    lines.append("## Pages With Context")
    if context_pages:
        for page in sorted(set(context_pages)):
            lines.append(f"- {page}")
    else:
        lines.append("- (none)")
    lines.append("")

    lines.append("## Pages Missing Context")
    for page in sorted(set(missing_context_pages)):
        lines.append(f"- {page}")
    lines.append("")

    lines.append("## Context Entries Missing Required Fields")
    if missing_required:
        for page, fields in missing_required.items():
            lines.append(f"- {page}: {', '.join(fields)}")
    else:
        lines.append("- (none)")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Audit written: {out_path}")
    print(f"pages_with_bot_marker={len(bot_pages)}")
    print(f"pages_with_context_entry={len(context_pages)}")
    print(f"context_entries_missing_required_fields={len(missing_required)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
