from typing import Any, Dict

JOBSPEC_SCHEMA: Dict[str, Any] = {
  "type": "object",
  "required": ["goal", "pipeline_stages", "markets", "outputs"],
  "additionalProperties": False,
  "properties": {
    "goal": {"type": "string"},
    "pipeline_stages": {"type": "array", "items": {"type": "string", "enum": ["permit_ingestion","normalization","crane_scoring","domain_resolution","contact_mining","email_generation","verification","sheets_export","edgar_prospecting","outreach_draft"]}},
    "markets": {"type": "array", "items": {"type": "string", "enum": ["chicago","dallas","phoenix","nyc","seattle","portland","harris_county_tx","all"]}},
    "vertical": {"type": "string", "enum": ["data_center","commercial","industrial","residential_highrise","energy","all"]},
    "outputs": {"type": "array", "items": {"type": "string", "enum": ["lead_csv","outreach_emails","google_sheets","run_report","jobspec_json"]}},
    "crane_score_threshold": {"type": "number", "minimum": 0, "maximum": 1},
    "max_leads": {"type": "integer", "minimum": 1},
    "icp_filter": {"type": "object", "properties": {"revenue_range": {"type": "string", "enum": ["$1M-$5M","$5M-$20M","$20M+","any"]}, "exclude_large_operators": {"type": "boolean"}}},
    "outreach_style": {"type": "string", "enum": ["hyper_specific","semi_personalized","generic"]},
    "notes": {"type": "string"}
  }
}

PERMIT_EXTRACTION_SCHEMA: Dict[str, Any] = {
  "type": "object",
  "required": ["project_address","city","permit_type","crane_likelihood_score"],
  "additionalProperties": False,
  "properties": {
    "project_address": {"type": "string"}, "city": {"type": "string"}, "state": {"type": "string"},
    "permit_number": {"type": "string"}, "permit_type": {"type": "string", "enum": ["new_construction","addition","renovation","demolition","other"]},
    "project_cost_usd": {"type": ["number","null"]}, "floor_count": {"type": ["integer","null"]},
    "building_use": {"type": "string"}, "general_contractor": {"type": ["string","null"]},
    "gc_contact_name": {"type": ["string","null"]}, "gc_contact_phone": {"type": ["string","null"]},
    "crane_likelihood_score": {"type": "number", "minimum": 0, "maximum": 1},
    "crane_likelihood_reasons": {"type": "array", "items": {"type": "string"}},
    "permit_date": {"type": ["string","null"]}, "estimated_lift_count": {"type": ["integer","null"]}
  }
}

OUTREACH_EMAIL_SCHEMA: Dict[str, Any] = {
  "type": "object",
  "required": ["subject","body","project_references"],
  "additionalProperties": False,
  "properties": {
    "subject": {"type": "string"}, "body": {"type": "string"},
    "project_references": {"type": "array", "items": {"type": "object", "properties": {"address": {"type": "string"}, "permit_number": {"type": "string"}, "project_type": {"type": "string"}}}},
    "tone": {"type": "string", "enum": ["direct","consultative","curious"]},
    "call_to_action": {"type": "string"}
  }
}

EDGAR_PROSPECT_SCHEMA: Dict[str, Any] = {
  "type": "object",
  "required": ["company_name","ticker","trigger_phrases_found","caa_fit_score"],
  "additionalProperties": False,
  "properties": {
    "company_name": {"type": "string"}, "ticker": {"type": "string"},
    "filing_type": {"type": "string", "enum": ["10-K","10-Q","8-K"]},
    "trigger_phrases_found": {"type": "array", "items": {"type": "string"}},
    "project_descriptions": {"type": "array", "items": {"type": "string"}},
    "caa_fit_score": {"type": "number", "minimum": 0, "maximum": 1},
    "recommended_tier": {"type": "string", "enum": ["priority_access","reserved_capacity","embedded_partner","not_a_fit"]},
    "outreach_angle": {"type": "string"}
  }
}
