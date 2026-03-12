-- contact_intelligence/schema/indexes.sql

-- source_records
CREATE INDEX IF NOT EXISTS idx_src_source_name    ON source_records(source_name);
CREATE INDEX IF NOT EXISTS idx_src_business_line  ON source_records(business_line);
CREATE INDEX IF NOT EXISTS idx_src_type           ON source_records(source_type);

-- sectors
CREATE INDEX IF NOT EXISTS idx_sector_name        ON sectors(sector_name);

-- companies
CREATE INDEX IF NOT EXISTS idx_co_norm_name       ON companies(normalized_company_name);
CREATE INDEX IF NOT EXISTS idx_co_domain          ON companies(domain);
CREATE INDEX IF NOT EXISTS idx_co_sector          ON companies(sector_id);
CREATE INDEX IF NOT EXISTS idx_co_state           ON companies(location_state);
CREATE INDEX IF NOT EXISTS idx_co_region          ON companies(region);
CREATE INDEX IF NOT EXISTS idx_co_tier            ON companies(target_tier);
CREATE INDEX IF NOT EXISTS idx_co_canonical       ON companies(canonical_company_id);

-- contacts
CREATE INDEX IF NOT EXISTS idx_ct_email           ON contacts(email);
CREATE INDEX IF NOT EXISTS idx_ct_verified        ON contacts(email_verified);
CREATE INDEX IF NOT EXISTS idx_ct_company         ON contacts(company_id);
CREATE INDEX IF NOT EXISTS idx_ct_full_name       ON contacts(full_name);
CREATE INDEX IF NOT EXISTS idx_ct_linkedin        ON contacts(linkedin_url);
CREATE INDEX IF NOT EXISTS idx_ct_state           ON contacts(location_state);
CREATE INDEX IF NOT EXISTS idx_ct_role            ON contacts(contact_role);

-- projects
CREATE INDEX IF NOT EXISTS idx_pr_company         ON projects(company_id);
CREATE INDEX IF NOT EXISTS idx_pr_sector          ON projects(sector_id);
CREATE INDEX IF NOT EXISTS idx_pr_state           ON projects(location_state);
CREATE INDEX IF NOT EXISTS idx_pr_type            ON projects(project_type);
CREATE INDEX IF NOT EXISTS idx_pr_stage           ON projects(project_stage);

-- crane_requirements
CREATE INDEX IF NOT EXISTS idx_cr_project         ON crane_requirements(project_id);
CREATE INDEX IF NOT EXISTS idx_cr_company         ON crane_requirements(company_id);

-- signals
CREATE INDEX IF NOT EXISTS idx_sig_company        ON signals(company_id);
CREATE INDEX IF NOT EXISTS idx_sig_type           ON signals(signal_type);

-- outreach_history
CREATE INDEX IF NOT EXISTS idx_oh_contact         ON outreach_history(contact_id);
CREATE INDEX IF NOT EXISTS idx_oh_company         ON outreach_history(company_id);
CREATE INDEX IF NOT EXISTS idx_oh_status          ON outreach_history(outreach_status);
CREATE INDEX IF NOT EXISTS idx_oh_business_line   ON outreach_history(business_line);

-- opportunities
CREATE INDEX IF NOT EXISTS idx_opp_company        ON opportunities(company_id);
CREATE INDEX IF NOT EXISTS idx_opp_status         ON opportunities(opportunity_status);

-- equipment_fleet
CREATE INDEX IF NOT EXISTS idx_fl_company         ON equipment_fleet(company_id);
CREATE INDEX IF NOT EXISTS idx_fl_state           ON equipment_fleet(location_state);
CREATE INDEX IF NOT EXISTS idx_fl_avail           ON equipment_fleet(availability_status);

-- canonical_companies
CREATE INDEX IF NOT EXISTS idx_cc_norm_name       ON canonical_companies(normalized_name);
CREATE INDEX IF NOT EXISTS idx_cc_domain          ON canonical_companies(primary_domain);

-- company_aliases
CREATE INDEX IF NOT EXISTS idx_alias_canonical    ON company_aliases(canonical_company_id);
CREATE INDEX IF NOT EXISTS idx_alias_norm         ON company_aliases(normalized_alias);

-- domain_evidence
CREATE INDEX IF NOT EXISTS idx_de_canonical       ON domain_evidence(canonical_company_id);
CREATE INDEX IF NOT EXISTS idx_de_domain          ON domain_evidence(domain_candidate);
CREATE INDEX IF NOT EXISTS idx_de_status          ON domain_evidence(verified_status);

-- contact_patterns
CREATE INDEX IF NOT EXISTS idx_cp_canonical       ON contact_patterns(canonical_company_id);
CREATE INDEX IF NOT EXISTS idx_cp_domain          ON contact_patterns(domain);

-- feedback_outcomes
CREATE INDEX IF NOT EXISTS idx_fb_company         ON feedback_outcomes(company_id);
CREATE INDEX IF NOT EXISTS idx_fb_contact         ON feedback_outcomes(contact_id);
CREATE INDEX IF NOT EXISTS idx_fb_type            ON feedback_outcomes(outcome_type);

-- gold_truth
CREATE INDEX IF NOT EXISTS idx_gt_domain          ON gold_truth_companies(verified_domain);
CREATE INDEX IF NOT EXISTS idx_gtc_gold           ON gold_truth_contacts(gold_id);

-- top targets
CREATE INDEX IF NOT EXISTS idx_tte_list           ON top_target_entries(list_id);
CREATE INDEX IF NOT EXISTS idx_tte_company        ON top_target_entries(company_id);

-- contact_source_facts
CREATE INDEX IF NOT EXISTS idx_csf_contact          ON contact_source_facts(contact_id);
CREATE INDEX IF NOT EXISTS idx_csf_company          ON contact_source_facts(company_id);
CREATE INDEX IF NOT EXISTS idx_csf_source_system    ON contact_source_facts(source_system);
CREATE INDEX IF NOT EXISTS idx_csf_usable           ON contact_source_facts(usable_for_outreach);
CREATE INDEX IF NOT EXISTS idx_csf_quality          ON contact_source_facts(record_quality_score);
CREATE INDEX IF NOT EXISTS idx_csf_verif            ON contact_source_facts(email_verification_status);

-- company_source_facts
CREATE INDEX IF NOT EXISTS idx_cof_company          ON company_source_facts(company_id);
CREATE INDEX IF NOT EXISTS idx_cof_source_system    ON company_source_facts(source_system);
CREATE INDEX IF NOT EXISTS idx_cof_pref_domain      ON company_source_facts(preferred_domain);
CREATE INDEX IF NOT EXISTS idx_cof_domain_conf      ON company_source_facts(domain_confidence);


-- jobs_feed_items
CREATE INDEX IF NOT EXISTS idx_jfi_state           ON jobs_feed_items(location_state);
CREATE INDEX IF NOT EXISTS idx_jfi_norm_company    ON jobs_feed_items(normalized_company_name);
CREATE INDEX IF NOT EXISTS idx_jfi_quality         ON jobs_feed_items(record_quality_score);

-- manpower_profiles
CREATE INDEX IF NOT EXISTS idx_mp_state            ON manpower_profiles(location_state);
CREATE INDEX IF NOT EXISTS idx_mp_name             ON manpower_profiles(normalized_full_name);
CREATE INDEX IF NOT EXISTS idx_mp_quality          ON manpower_profiles(record_quality_score);

-- opportunity_feed_items
CREATE INDEX IF NOT EXISTS idx_ofi_state           ON opportunity_feed_items(location_state);
CREATE INDEX IF NOT EXISTS idx_ofi_type            ON opportunity_feed_items(opportunity_type);
CREATE INDEX IF NOT EXISTS idx_ofi_quality         ON opportunity_feed_items(record_quality_score);

-- match tables
CREATE INDEX IF NOT EXISTS idx_jcm_job             ON job_contact_matches(job_feed_id);
CREATE INDEX IF NOT EXISTS idx_jcm_contact         ON job_contact_matches(contact_id);
CREATE INDEX IF NOT EXISTS idx_ocm_opp             ON opportunity_company_matches(opportunity_feed_id);
CREATE INDEX IF NOT EXISTS idx_ocm_company         ON opportunity_company_matches(company_id);
CREATE INDEX IF NOT EXISTS idx_mjm_profile         ON manpower_job_matches(manpower_profile_id);
CREATE INDEX IF NOT EXISTS idx_mjm_job             ON manpower_job_matches(job_feed_id);
