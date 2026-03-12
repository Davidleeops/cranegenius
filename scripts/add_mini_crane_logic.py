from pathlib import Path
import shutil
import sys

TARGET = Path("contact_intelligence/scripts/build_project_intelligence.py")

MINI_INIT_BLOCK = '''
mini_opportunity_candidates = []
'''.strip()

MINI_KEYWORDS_BLOCK = '''
    mini_keywords = [
        "curtain wall",
        "glass installation",
        "glazing",
        "storefront",
        "rooftop hvac",
        "hvac replacement",
        "mechanical retrofit",
        "interior equipment",
        "tight access lift",
        "equipment install",
        "hospital retrofit",
        "facade replacement",
        "window replacement",
        "mechanical upgrade",
        "glass replacement",
        "unitized curtain wall",
        "facade",
        "window wall",
        "rooftop unit",
        "rtu replacement",
        "chiller replacement",
        "air handler",
        "mechanical equipment"
    ]

    text_blob = " ".join([
        str(record.get("project_name", "")),
        str(record.get("description", "")),
        str(record.get("industry", "")),
        str(record.get("signal_type", "")),
        str(record.get("scope", "")),
        str(record.get("notes", ""))
    ]).lower()

    is_mini_opportunity = any(k in text_blob for k in mini_keywords)

    if is_mini_opportunity:
        record["opportunity_type"] = "mini_crane"
'''.strip("\n")

MINI_APPEND_BLOCK = '''
    if record.get("opportunity_type") == "mini_crane":
        mini_opportunity_candidates.append(record)
'''.strip("\n")


def fail(msg: str) -> None:
    print(f"ERROR: {msg}")
    sys.exit(1)


def main() -> None:
    if not TARGET.exists():
        fail(f"Target file not found: {TARGET}")

    original = TARGET.read_text(encoding="utf-8")

    if 'record["opportunity_type"] = "mini_crane"' in original:
        print("Mini crane logic already appears to exist. No changes made.")
        return

    updated = original

    backup_path = TARGET.with_suffix(TARGET.suffix + ".bak")
    shutil.copy2(TARGET, backup_path)
    print(f"Backup created: {backup_path}")

    changed = False

    if "mini_opportunity_candidates = []" not in updated:
        init_anchor_candidates = [
            "recommended_expansion_candidates = []",
            "top_logistics_candidates = []",
            "top_energy_candidates = []",
            "top_project_candidates = []",
            "national_opportunity_candidates = []",
            "project_candidates = []",
            "signal_events = []",
        ]
        inserted = False
        for anchor in init_anchor_candidates:
            if anchor in updated:
                updated = updated.replace(anchor, anchor + "\n" + MINI_INIT_BLOCK, 1)
                inserted = True
                changed = True
                print(f"Inserted mini_opportunity_candidates initialization after: {anchor}")
                break
        if not inserted:
            fail("Could not find candidate list initialization anchor.")

    classifier_anchor = "project_type = classify_project_type(record)"
    if classifier_anchor in updated:
        replacement = classifier_anchor + "\n" + MINI_KEYWORDS_BLOCK
        updated = updated.replace(classifier_anchor, replacement, 1)
        changed = True
        print("Inserted mini crane classifier block after project_type assignment.")
    else:
        fallback_anchors = [
            'record["project_type"] = project_type',
            "record['project_type'] = project_type",
            "project_candidates.append(record)",
        ]
        inserted = False
        for anchor in fallback_anchors:
            if anchor in updated:
                updated = updated.replace(anchor, MINI_KEYWORDS_BLOCK + "\n" + anchor, 1)
                inserted = True
                changed = True
                print(f"Inserted mini crane classifier block before: {anchor}")
                break
        if not inserted:
            fail("Could not find classification anchor to insert mini crane logic.")

    if "mini_opportunity_candidates.append(record)" not in updated:
        append_anchor_candidates = [
            "recommended_expansion_candidates.append(record)",
            "national_opportunity_candidates.append(record)",
            "project_candidates.append(record)",
        ]
        inserted = False
        for anchor in append_anchor_candidates:
            if anchor in updated:
                updated = updated.replace(anchor, anchor + "\n" + MINI_APPEND_BLOCK, 1)
                inserted = True
                changed = True
                print(f"Inserted mini append block after: {anchor}")
                break
        if not inserted:
            fail("Could not find append anchor for mini opportunity candidates.")

    export_present = "mini_opportunity_candidates" in updated and "mini_opportunity_candidates.json" in updated
    if not export_present:
        export_anchor_candidates = [
            'write_json_export("recommended_expansion_candidates.json", recommended_expansion_candidates)',
            "write_json_export('recommended_expansion_candidates.json', recommended_expansion_candidates)",
            'write_json_export("top_logistics_candidates.json", top_logistics_candidates)',
            "write_json_export('top_logistics_candidates.json', top_logistics_candidates)",
            'write_json_export("top_project_candidates.json", top_project_candidates)',
            "write_json_export('top_project_candidates.json', top_project_candidates)",
        ]
        inserted = False
        for anchor in export_anchor_candidates:
            if anchor in updated:
                export_block = 'write_json_export("mini_opportunity_candidates.json", mini_opportunity_candidates)'
                updated = updated.replace(anchor, anchor + "\n" + export_block, 1)
                inserted = True
                changed = True
                print(f"Inserted mini opportunity export after: {anchor}")
                break
        if not inserted:
            fail("Could not find export anchor for mini opportunity candidates.")

    if not changed:
        print("No changes were necessary.")
        return

    TARGET.write_text(updated, encoding="utf-8")
    print(f"Patched file: {TARGET}")
    print("Done.")


if __name__ == "__main__":
    main()
