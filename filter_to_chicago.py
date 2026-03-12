import sqlite3
from pathlib import Path

DB = Path.home() / "data_runtime" / "cranegenius_ci.db"
conn = sqlite3.connect(DB)
cur = conn.cursor()

# Count before
total_co = cur.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
total_ct = cur.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
print(f"Before: {total_co} companies, {total_ct} contacts")

# Delete TX companies and their contacts
cur.execute("DELETE FROM contacts WHERE company_id IN (SELECT company_id FROM companies WHERE location_state='TX')")
ct_deleted = conn.total_changes
cur.execute("DELETE FROM companies WHERE location_state='TX'")
co_deleted = conn.total_changes - ct_deleted

conn.commit()
print(f"Removed: {co_deleted} TX companies, {ct_deleted} TX contacts")

remaining_co = cur.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
remaining_ct = cur.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
print(f"Remaining: {remaining_co} companies, {remaining_ct} contacts")

print("\nRemaining companies:")
for r in cur.execute("SELECT company_name, domain, location_state FROM companies LIMIT 10").fetchall():
    print(f"  {str(r[0])[:35]:35} | {str(r[1] or '')[:28]:28} | {r[2]}")

print("\nVerification status of remaining contacts:")
for r in cur.execute("SELECT email_verified, COUNT(*) n FROM contacts GROUP BY email_verified").fetchall():
    label = "verified" if r[0] else "unverified"
    print(f"  {label}: {r[1]}")

conn.close()
print("\nDone. Now run: python3 contact_intelligence/scripts/export_views.py")
