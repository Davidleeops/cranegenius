-- contact_intelligence/schema/seed_sectors.sql
-- 20 crane-demand verticals seeded at init time

INSERT OR IGNORE INTO sectors (sector_name, display_order) VALUES
    ('Data Center Construction', 1),
    ('Semiconductor Manufacturing', 2),
    ('Utility Grid Infrastructure', 3),
    ('LNG and Natural Gas Infrastructure', 4),
    ('Wind Energy Projects', 5),
    ('Battery and EV Manufacturing', 6),
    ('Industrial Shutdown and Turnaround', 7),
    ('Nuclear Plant Refurbishment', 8),
    ('Solar Farm Construction', 9),
    ('Port and Maritime Infrastructure', 10),
    ('Bridge and Civil Infrastructure', 11),
    ('Highway Infrastructure', 12),
    ('Airport Construction and Expansion', 13),
    ('Modular and Prefabricated Construction', 14),
    ('Petrochemical Facilities', 15),
    ('Steel and Heavy Manufacturing', 16),
    ('Water Treatment Infrastructure', 17),
    ('LNG Storage Tank Construction', 18),
    ('Mining Infrastructure', 19),
    ('Disaster Recovery Infrastructure', 20);
