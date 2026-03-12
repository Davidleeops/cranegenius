-- Project Intelligence Layer (additive extension)

CREATE TABLE IF NOT EXISTS signal_source_runs (
    signal_source_run_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name            TEXT NOT NULL,
    source_type            TEXT,
    run_started_at         DATETIME DEFAULT CURRENT_TIMESTAMP,
    run_finished_at        DATETIME,
    records_seen           INTEGER DEFAULT 0,
    records_ingested       INTEGER DEFAULT 0,
    status                 TEXT DEFAULT 'success',
    notes                  TEXT
);

CREATE TABLE IF NOT EXISTS signal_events (
    signal_event_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_key             TEXT NOT NULL UNIQUE,
    source_name            TEXT NOT NULL,
    source_record_id       TEXT,
    source_url             TEXT,
    signal_type            TEXT NOT NULL,
    project_name_raw       TEXT,
    company_name_raw       TEXT,
    city                   TEXT,
    state                  TEXT,
    observed_at            TEXT,
    effective_date         TEXT,
    confidence_score       REAL DEFAULT 0.0,
    raw_payload_ref        TEXT,
    raw_payload            TEXT,
    created_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at             DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS project_candidates (
    project_candidate_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_key             TEXT NOT NULL UNIQUE,
    project_name_raw          TEXT,
    project_name_normalized   TEXT,
    project_type              TEXT,
    vertical                  TEXT,
    city                      TEXT,
    state                     TEXT,
    metro                     TEXT,
    company_name_raw          TEXT,
    company_name_normalized   TEXT,
    source_count              INTEGER DEFAULT 0,
    signal_count              INTEGER DEFAULT 0,
    earliest_signal_date      TEXT,
    latest_signal_date        TEXT,
    estimated_spend_proxy     REAL DEFAULT 0.0,
    crane_relevance_score     REAL DEFAULT 0.0,
    mini_crane_fit_score      REAL DEFAULT 0.0,
    demand_score              REAL DEFAULT 0.0,
    timing_score              REAL DEFAULT 0.0,
    matchability_score        REAL DEFAULT 0.0,
    monetization_score        REAL DEFAULT 0.0,
    confidence_score          REAL DEFAULT 0.0,
    recommended_flag          INTEGER DEFAULT 0,
    recommendation_reason     TEXT,
    priority_reason           TEXT,
    status                    TEXT DEFAULT 'active',
    created_at                DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at                DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS project_signal_links (
    project_signal_link_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    project_candidate_id    INTEGER NOT NULL REFERENCES project_candidates(project_candidate_id),
    signal_event_id         INTEGER NOT NULL REFERENCES signal_events(signal_event_id),
    link_strength           REAL DEFAULT 0.0,
    link_reason             TEXT,
    created_at              DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(project_candidate_id, signal_event_id)
);

-- Source-specific normalized items
CREATE TABLE IF NOT EXISTS permit_signal_items (
    permit_signal_item_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    source_item_key         TEXT NOT NULL UNIQUE,
    source_name             TEXT,
    permit_id               TEXT,
    description             TEXT,
    address                 TEXT,
    city                    TEXT,
    state                   TEXT,
    issued_at               TEXT,
    source_url              TEXT,
    raw_payload             TEXT,
    created_at              DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS jobs_signal_items (
    jobs_signal_item_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    source_item_key         TEXT NOT NULL UNIQUE,
    source_name             TEXT,
    job_title               TEXT,
    company_name            TEXT,
    location                TEXT,
    city                    TEXT,
    state                   TEXT,
    employment_type         TEXT,
    description             TEXT,
    posted_at               TEXT,
    source_url              TEXT,
    raw_payload             TEXT,
    created_at              DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS procurement_signal_items (
    procurement_signal_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_item_key         TEXT NOT NULL UNIQUE,
    source_name             TEXT,
    notice_title            TEXT,
    agency_name             TEXT,
    city                    TEXT,
    state                   TEXT,
    due_date                TEXT,
    source_url              TEXT,
    raw_payload             TEXT,
    created_at              DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS faa_notice_items (
    faa_notice_item_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    source_item_key         TEXT NOT NULL UNIQUE,
    source_name             TEXT,
    notice_id               TEXT,
    notice_title            TEXT,
    city                    TEXT,
    state                   TEXT,
    effective_date          TEXT,
    source_url              TEXT,
    raw_payload             TEXT,
    created_at              DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS company_news_signal_items (
    company_news_signal_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_item_key         TEXT NOT NULL UNIQUE,
    source_name             TEXT,
    headline                TEXT,
    company_name            TEXT,
    city                    TEXT,
    state                   TEXT,
    published_at            TEXT,
    source_url              TEXT,
    raw_payload             TEXT,
    created_at              DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_se_source         ON signal_events(source_name);
CREATE INDEX IF NOT EXISTS idx_se_state          ON signal_events(state);
CREATE INDEX IF NOT EXISTS idx_se_type           ON signal_events(signal_type);
CREATE INDEX IF NOT EXISTS idx_pc_vertical       ON project_candidates(vertical);
CREATE INDEX IF NOT EXISTS idx_pc_state          ON project_candidates(state);
CREATE INDEX IF NOT EXISTS idx_pc_monetization   ON project_candidates(monetization_score);
CREATE INDEX IF NOT EXISTS idx_pc_confidence     ON project_candidates(confidence_score);
CREATE INDEX IF NOT EXISTS idx_psl_project       ON project_signal_links(project_candidate_id);
CREATE INDEX IF NOT EXISTS idx_psl_signal        ON project_signal_links(signal_event_id);
