-- ============================================================
-- CraneGenius Contact Intelligence System v2
-- create_tables.sql
-- Three layers: CRM/Marketplace | Reference/Memory | Learning/Eval
-- Run via: python3 scripts/init_db.py
-- ============================================================

PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- ============================================================
-- LAYER 0: SOURCE RECORDS (raw imports, never modified)
-- ============================================================

CREATE TABLE IF NOT EXISTS source_records (
    source_record_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name           TEXT NOT NULL,
    source_type           TEXT NOT NULL,   -- permit|apollo|manual|linkedin|edgar|seed
    business_line         TEXT NOT NULL DEFAULT 'cranegenius',
    original_company_name TEXT,
    original_person_name  TEXT,
    original_email        TEXT,
    original_phone        TEXT,
    raw_payload           TEXT,            -- full JSON of original row
    imported_at           DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- LAYER 1: CORE CRM AND MARKETPLACE
-- ============================================================

CREATE TABLE IF NOT EXISTS sectors (
    sector_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    sector_name    TEXT NOT NULL UNIQUE,   -- Data Center, Wind Energy, LNG, etc.
    display_order  INTEGER DEFAULT 0,
    active         INTEGER DEFAULT 1,
    notes          TEXT
);

CREATE TABLE IF NOT EXISTS companies (
    company_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_company_id    INTEGER REFERENCES canonical_companies(canonical_company_id),
    sector_id               INTEGER REFERENCES sectors(sector_id),
    company_name            TEXT NOT NULL,
    normalized_company_name TEXT,
    company_type            TEXT,          -- GC|subcontractor|owner|crane_rental|EPC|developer
    domain                  TEXT,
    linkedin_company_url    TEXT,
    industry                TEXT,
    subindustry             TEXT,
    location_city           TEXT,
    location_state          TEXT,
    region                  TEXT,          -- Southwest|Midwest|Southeast|Northeast|West
    country                 TEXT DEFAULT 'US',
    employee_count          INTEGER,
    revenue_band            TEXT,
    target_tier             INTEGER,       -- 1=Tier1, 2=Tier2, 3=Tier3
    target_score            REAL DEFAULT 0.0,
    priority_reason         TEXT,
    notes                   TEXT,
    created_at              DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at              DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS contacts (
    contact_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id           INTEGER REFERENCES companies(company_id),
    full_name            TEXT,
    first_name           TEXT,
    last_name            TEXT,
    contact_role         TEXT,             -- procurement|operations|pm|estimator|executive
    title                TEXT,
    department           TEXT,
    seniority            TEXT,             -- c_suite|vp|director|manager|individual
    email                TEXT,
    email_type           TEXT,             -- work|personal|generic
    email_verified       INTEGER DEFAULT 0,
    email_pattern        TEXT,             -- {first}.{last}@domain.com
    phone                TEXT,
    phone_type           TEXT,
    linkedin_url         TEXT,
    location_city        TEXT,
    location_state       TEXT,
    confidence_score     REAL DEFAULT 0.0,
    lead_score           REAL DEFAULT 0.0,
    last_verified_at     DATETIME,
    notes                TEXT,
    created_at           DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at           DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS projects (
    project_id               INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id               INTEGER REFERENCES companies(company_id),
    sector_id                INTEGER REFERENCES sectors(sector_id),
    project_name             TEXT,
    project_type             TEXT,         -- data_center|industrial|utility|wind|solar|LNG|nuclear|bridge|highway|port|modular
    market_segment           TEXT,
    project_location         TEXT,
    location_city            TEXT,
    location_state           TEXT,
    region                   TEXT,
    project_stage            TEXT,         -- planning|permit_filed|under_construction|completed
    estimated_project_value  REAL,
    timeline                 TEXT,
    project_start_date       TEXT,
    project_end_date         TEXT,
    general_contractor       TEXT,
    subcontractor            TEXT,
    permit_number            TEXT,
    permit_source            TEXT,
    crane_need_score         INTEGER DEFAULT 0,
    urgency_score            INTEGER DEFAULT 0,
    notes                    TEXT,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS project_contacts (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES projects(project_id),
    contact_id INTEGER NOT NULL REFERENCES contacts(contact_id),
    role_on_project TEXT
);

CREATE TABLE IF NOT EXISTS crane_requirements (
    requirement_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id             INTEGER REFERENCES projects(project_id),
    company_id             INTEGER REFERENCES companies(company_id),
    crane_type_needed      TEXT,           -- crawler|mobile|tower|RT|all_terrain|spider
    capacity_required      TEXT,           -- e.g. "200 ton"
    boom_length_required   TEXT,
    estimated_lift_count   INTEGER,
    start_date_needed      TEXT,
    duration_weeks         INTEGER,
    notes                  TEXT
);

CREATE TABLE IF NOT EXISTS signals (
    signal_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id        INTEGER REFERENCES companies(company_id),
    project_id        INTEGER REFERENCES projects(project_id),
    signal_type       TEXT NOT NULL,      -- permit_filed|hiring|fleet_growth|expansion|contract_award|turnaround|shutdown
    signal_value      TEXT,
    signal_date       TEXT,
    signal_confidence REAL DEFAULT 0.5,
    source            TEXT,
    source_url        TEXT,
    captured_at       DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS outreach_history (
    outreach_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id         INTEGER REFERENCES contacts(contact_id),
    company_id         INTEGER REFERENCES companies(company_id),
    opportunity_id     INTEGER REFERENCES opportunities(opportunity_id),
    campaign_name      TEXT,
    business_line      TEXT DEFAULT 'cranegenius',
    message_angle      TEXT,
    outreach_channel   TEXT,              -- email|linkedin|phone|referral
    outreach_status    TEXT DEFAULT 'not_sent',
    last_contacted     DATETIME,
    response_status    TEXT DEFAULT 'none', -- none|replied|bounced|unsubscribed|meeting_set
    notes              TEXT
);

CREATE TABLE IF NOT EXISTS opportunities (
    opportunity_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id         INTEGER REFERENCES companies(company_id),
    project_id         INTEGER REFERENCES projects(project_id),
    requirement_id     INTEGER REFERENCES crane_requirements(requirement_id),
    contact_id         INTEGER REFERENCES contacts(contact_id),
    opportunity_type   TEXT,              -- crane_rental|manpower|equipment_sale|CAA|retainer
    opportunity_status TEXT DEFAULT 'none', -- none|qualified|proposal|negotiation|closed_won|closed_lost
    probability_score  REAL DEFAULT 0.0,
    estimated_value    REAL,
    close_date_target  TEXT,
    notes              TEXT,
    created_at         DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at         DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS deal_pipeline (
    deal_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    opportunity_id  INTEGER REFERENCES opportunities(opportunity_id),
    stage           TEXT,                 -- lead|qualified|proposal|negotiation|closed_won|closed_lost
    stage_date      DATETIME DEFAULT CURRENT_TIMESTAMP,
    deal_value      REAL,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS equipment_fleet (
    fleet_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id           INTEGER REFERENCES companies(company_id),
    crane_type           TEXT,
    crane_model          TEXT,
    manufacturer         TEXT,
    max_capacity         TEXT,
    boom_length          TEXT,
    year_manufactured    INTEGER,
    availability_status  TEXT DEFAULT 'unknown', -- available|deployed|maintenance|unknown
    next_available_date  TEXT,
    location_city        TEXT,
    location_state       TEXT,
    certifications       TEXT,
    travel_available     INTEGER DEFAULT 0,
    notes                TEXT
);

CREATE TABLE IF NOT EXISTS operator_network (
    operator_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id           INTEGER REFERENCES companies(company_id),
    full_name            TEXT,
    license_type         TEXT,
    certifications       TEXT,
    crane_types          TEXT,
    location_city        TEXT,
    location_state       TEXT,
    travel_available     INTEGER DEFAULT 0,
    availability_status  TEXT DEFAULT 'unknown',
    next_available_date  TEXT,
    email                TEXT,
    phone                TEXT,
    notes                TEXT
);

-- ============================================================
-- LAYER 2: REFERENCE DATA AND MEMORY
-- ============================================================

CREATE TABLE IF NOT EXISTS canonical_companies (
    canonical_company_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name          TEXT NOT NULL,
    normalized_name         TEXT,
    primary_domain          TEXT,
    sector_id               INTEGER REFERENCES sectors(sector_id),
    company_type            TEXT,
    location_city           TEXT,
    location_state          TEXT,
    verified_status         TEXT DEFAULT 'unverified',  -- unverified|verified|rejected
    promoted_from           TEXT,          -- which source_type or source_name promoted this
    created_at              DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at              DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS company_aliases (
    alias_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_company_id INTEGER NOT NULL REFERENCES canonical_companies(canonical_company_id),
    alias_name           TEXT NOT NULL,
    normalized_alias     TEXT,
    source               TEXT,
    confidence           REAL DEFAULT 0.5
);

CREATE TABLE IF NOT EXISTS domain_evidence (
    evidence_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_company_id INTEGER REFERENCES canonical_companies(canonical_company_id),
    company_id           INTEGER REFERENCES companies(company_id),
    domain_candidate     TEXT NOT NULL,
    evidence_type        TEXT,            -- website|linkedin|directory|email_sig|permit|manual|scraped
    evidence_score       REAL DEFAULT 0.0,
    confidence_score     REAL DEFAULT 0.0,
    verified_status      TEXT DEFAULT 'candidate', -- candidate|verified|rejected
    last_verified_at     DATETIME,
    source_url           TEXT,
    notes                TEXT,
    created_at           DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS contact_patterns (
    pattern_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_company_id INTEGER REFERENCES canonical_companies(canonical_company_id),
    company_id           INTEGER REFERENCES companies(company_id),
    domain               TEXT,
    pattern_template     TEXT,            -- {first}.{last} | {first_initial}{last} | {first}
    pattern_example      TEXT,            -- jane.smith@acme.com
    verified_count       INTEGER DEFAULT 0,
    bounce_count         INTEGER DEFAULT 0,
    confidence           REAL DEFAULT 0.5,
    source               TEXT,
    last_updated         DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS source_registry (
    source_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name    TEXT NOT NULL UNIQUE,
    source_type    TEXT,                  -- permit|apollo|manual|linkedin|edgar|seed
    source_quality REAL DEFAULT 0.5,      -- 0.0–1.0
    description    TEXT,
    active         INTEGER DEFAULT 1,
    last_import    DATETIME,
    notes          TEXT
);

-- ============================================================
-- LAYER 3: LEARNING AND EVALUATION
-- ============================================================

CREATE TABLE IF NOT EXISTS feedback_outcomes (
    outcome_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id           INTEGER REFERENCES companies(company_id),
    contact_id           INTEGER REFERENCES contacts(contact_id),
    canonical_company_id INTEGER REFERENCES canonical_companies(canonical_company_id),
    opportunity_id       INTEGER REFERENCES opportunities(opportunity_id),
    domain_tested        TEXT,
    email_tested         TEXT,
    outcome_type         TEXT NOT NULL,   -- bounce|reply|wrong_person|verified_domain|false_positive|meeting_set
    outcome_detail       TEXT,
    recorded_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    source               TEXT
);

CREATE TABLE IF NOT EXISTS gold_truth_companies (
    gold_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name       TEXT NOT NULL,
    verified_domain      TEXT,
    verified_city        TEXT,
    verified_state       TEXT,
    sector               TEXT,
    company_type         TEXT,
    source               TEXT,
    verified_at          DATETIME,
    notes                TEXT
);

CREATE TABLE IF NOT EXISTS gold_truth_contacts (
    gold_contact_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    gold_id              INTEGER REFERENCES gold_truth_companies(gold_id),
    full_name            TEXT,
    title                TEXT,
    verified_email       TEXT,
    verified_phone       TEXT,
    linkedin_url         TEXT,
    source               TEXT,
    verified_at          DATETIME,
    notes                TEXT
);

-- ============================================================
-- TOP 100 TARGET LISTS
-- ============================================================

CREATE TABLE IF NOT EXISTS top_target_lists (
    list_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    list_name      TEXT NOT NULL,         -- "Top 100 Data Center GCs Texas"
    sector_id      INTEGER REFERENCES sectors(sector_id),
    region         TEXT,
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    notes          TEXT
);

CREATE TABLE IF NOT EXISTS top_target_entries (
    entry_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    list_id        INTEGER NOT NULL REFERENCES top_target_lists(list_id),
    company_id     INTEGER REFERENCES companies(company_id),
    rank           INTEGER,
    score          REAL DEFAULT 0.0,
    priority_reason TEXT,
    opportunity_notes TEXT,
    added_at       DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- SOURCE QUALITY + PROVENANCE FACTS (cross-source usability model)
-- ============================================================

CREATE TABLE IF NOT EXISTS contact_source_facts (
    contact_source_fact_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id                INTEGER NOT NULL REFERENCES contacts(contact_id),
    company_id                INTEGER REFERENCES companies(company_id),
    source_record_id          INTEGER REFERENCES source_records(source_record_id),
    source_system             TEXT,
    source_file               TEXT,
    source_sheet              TEXT,
    source_row                INTEGER,
    ingested_at               DATETIME DEFAULT CURRENT_TIMESTAMP,
    first_seen_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_seen_at              DATETIME DEFAULT CURRENT_TIMESTAMP,
    email_verification_status TEXT,
    email_domain_type         TEXT,
    title_confidence          REAL DEFAULT 0.0,
    person_confidence         REAL DEFAULT 0.0,
    company_confidence        REAL DEFAULT 0.0,
    record_quality_score      REAL DEFAULT 0.0,
    usable_for_outreach       INTEGER DEFAULT 0,
    usable_reason             TEXT,
    blocked_reason            TEXT,
    notes                     TEXT,
    UNIQUE(contact_id, source_system, source_file, source_sheet, source_row)
);

CREATE TABLE IF NOT EXISTS company_source_facts (
    company_source_fact_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id                INTEGER NOT NULL REFERENCES companies(company_id),
    source_record_id          INTEGER REFERENCES source_records(source_record_id),
    source_system             TEXT,
    source_file               TEXT,
    source_sheet              TEXT,
    source_row                INTEGER,
    ingested_at               DATETIME DEFAULT CURRENT_TIMESTAMP,
    first_seen_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_seen_at              DATETIME DEFAULT CURRENT_TIMESTAMP,
    source_support_count      INTEGER DEFAULT 1,
    preferred_domain          TEXT,
    domain_confidence         REAL DEFAULT 0.0,
    domain_evidence_notes     TEXT,
    quality_notes             TEXT,
    notes                     TEXT,
    UNIQUE(company_id, source_system, preferred_domain)
);


-- ============================================================
-- LAYER 1B: FEED NORMALIZATION + MATCH CANDIDATES (PHASE 2)
-- ============================================================

CREATE TABLE IF NOT EXISTS jobs_feed_items (
    job_feed_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    external_key           TEXT NOT NULL UNIQUE,
    source                 TEXT,
    source_url             TEXT,
    title                  TEXT NOT NULL,
    company_name           TEXT,
    normalized_company_name TEXT,
    location_text          TEXT,
    location_state         TEXT,
    employment_type        TEXT,
    description            TEXT,
    posted_at              TEXT,
    record_quality_score   REAL DEFAULT 0.0,
    created_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at             DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS manpower_profiles (
    manpower_profile_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    external_key           TEXT NOT NULL UNIQUE,
    source                 TEXT,
    source_url             TEXT,
    full_name              TEXT,
    normalized_full_name   TEXT,
    title                  TEXT,
    certifications         TEXT,
    location_state         TEXT,
    availability_status    TEXT,
    email                  TEXT,
    phone                  TEXT,
    record_quality_score   REAL DEFAULT 0.0,
    created_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at             DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS opportunity_feed_items (
    opportunity_feed_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    external_key           TEXT NOT NULL UNIQUE,
    source                 TEXT,
    source_url             TEXT,
    project_name           TEXT,
    normalized_project_name TEXT,
    city                   TEXT,
    location_state         TEXT,
    opportunity_type       TEXT,
    project_stage          TEXT,
    summary                TEXT,
    captured_at            TEXT,
    record_quality_score   REAL DEFAULT 0.0,
    created_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at             DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS job_contact_matches (
    job_contact_match_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    job_feed_id            INTEGER NOT NULL REFERENCES jobs_feed_items(job_feed_id),
    contact_id             INTEGER NOT NULL REFERENCES contacts(contact_id),
    company_id             INTEGER REFERENCES companies(company_id),
    match_score            REAL DEFAULT 0.0,
    match_reason           TEXT,
    created_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_feed_id, contact_id)
);

CREATE TABLE IF NOT EXISTS opportunity_company_matches (
    opportunity_company_match_id INTEGER PRIMARY KEY AUTOINCREMENT,
    opportunity_feed_id     INTEGER NOT NULL REFERENCES opportunity_feed_items(opportunity_feed_id),
    company_id              INTEGER NOT NULL REFERENCES companies(company_id),
    match_score             REAL DEFAULT 0.0,
    match_reason            TEXT,
    created_at              DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(opportunity_feed_id, company_id)
);

CREATE TABLE IF NOT EXISTS manpower_job_matches (
    manpower_job_match_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    manpower_profile_id     INTEGER NOT NULL REFERENCES manpower_profiles(manpower_profile_id),
    job_feed_id             INTEGER NOT NULL REFERENCES jobs_feed_items(job_feed_id),
    match_score             REAL DEFAULT 0.0,
    match_reason            TEXT,
    created_at              DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(manpower_profile_id, job_feed_id)
);
