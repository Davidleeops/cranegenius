"""
CraneGenius Stage 9 — AI Email Writer
Pulls from hot, warm, AND catchall lists.
"""
from __future__ import annotations
import os, re, json
import pandas as pd
import requests
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

SYSTEM_PROMPT = """You write cold outreach emails for CraneGenius using the Hormozi offer framework.

CraneGenius is a crane intelligence platform founded by David Lee — a former Operating Engineer 
who knows crane logistics from the field. CraneGenius does NOT rent cranes. We do the sourcing 
legwork so GCs never have to: we identify crane options, find cost-saving alternatives, flag 
what's available without lead time delays, and deliver a shortlist same day. GCs reply once. 
We do the rest.

Writing rules:
- CRITICAL: Never attribute savings, time cuts, or outcomes to any named company, team, person, or project. This is non-negotiable. Use only unverifiable general industry patterns.
- Always specific, never fluffy. Sound like a human, not a bot or a salesperson.
- 4 sentences max in the body. No fluff sentences.
- Structure every email exactly like this:
  1. HOOK: "I saw your permit for [exact address]" — proves you did homework
  2. PROBLEM: One sentence on the pain (sourcing cranes wastes hours, delays projects)  
  3. OFFER: What CraneGenius does for them — we can pull options for their exact project 
     same day, find cheaper alternatives, flag what has no lead time delays. They reply once, 
     we do all the work. Zero commitment. Saves time, money, and brain space.
     Do NOT say "I already pulled options" — say "we can pull options same day" instead.
  4. FREE LINE: "We get paid by crane companies, not GCs — so the shortlist costs you nothing." 
     This must appear in every email. It kills the "what's the catch" objection.
  5. CTA: Single low-friction ask — reply "yes" and I send options same day. 
     One word to claim it. Binary, zero commitment.
- Never say "I wanted to reach out" or "I hope this finds you well"
- Never say "we connect GCs with crane companies" — say what we DO for them instead
- Never use placeholders like [Name] or [Company]
- David's former Operating Engineer background = credibility, use it naturally once
- Always include a Subject line as the first line
- Sign off must be exactly these 3 lines, nothing else:
  David Lee, PMP
  Former Operating Engineer | Founder, CraneGenius
  cranegenius.com"""

def write_email(company, address, city, state, triggers, score):
    trigger_clean = str(triggers).replace('_',' ').replace(',',' + ')
    address_clean = address.title().strip()
    prompt = f"""Write a cold outreach email to a GC we found via building permit.

Company: {company}
Project address: {address_clean}, {city}, {state}
Project signals: {trigger_clean}
Crane likelihood score: {score}/10

Follow the system prompt framework exactly.
First line MUST be: Subject: [subject here]
Then blank line, then email body (4 sentences max), then signature."""

    api_key = os.environ.get('ANTHROPIC_API_KEY')
    r = requests.post(
        'https://api.anthropic.com/v1/messages',
        headers={
            'x-api-key': api_key,
            'anthropic-version': '2023-06-01',
            'content-type': 'application/json'
        },
        json={
            'model': 'claude-sonnet-4-20250514',
            'max_tokens': 350,
            'system': SYSTEM_PROMPT,
            'messages': [{'role': 'user', 'content': prompt}]
        },
        timeout=30
    )
    data = r.json()
    if 'error' in data:
        raise Exception(data['error'])
    return ''.join(
        b.get('text','') for b in data.get('content',[])
        if b.get('type') == 'text'
    ).strip()

def parse_email(raw_text):
    lines = raw_text.strip().split('\n')
    subject = ''
    body_lines = []
    for i, line in enumerate(lines):
        if line.lower().startswith('subject:'):
            subject = line.split(':', 1)[1].strip()
        elif subject and line.strip():
            body_lines = lines[i:]
            break
    body = '\n'.join(body_lines).strip()
    for dash_char in [chr(0x2014), chr(0x2013), chr(0x2012), chr(0x2015)]:
        body = body.replace(dash_char, ',')
        subject = subject.replace(dash_char, '')
    return subject, body

def get_company_name(row):
    for col in ['contractor_name_normalized_x', 'contractor_name_raw', 'contractor_name_normalized_y']:
        val = str(row.get(col, '') or '').strip()
        if val and val not in ('nan', ''):
            return val.split(',')[0].strip().title()
    domain = str(row.get('contractor_domain', '') or '')
    return domain.replace('.com','').replace('.net','').replace('.org','').title()

def run_email_writer():
    out_path = 'data/outreach_emails.csv'
    dfs = []
    for path, tier in [
        ('data/sender_ready_hot.csv', 'hot'),
        ('data/sender_ready_warm.csv', 'warm'),
        ('data/catchall_review.csv', 'catchall'),
    ]:
        if os.path.exists(path):
            df = pd.read_csv(path)
            df['tier'] = tier
            dfs.append(df)
            print(f"  Loaded {len(df)} rows from {path}")

    if not dfs:
        print("No sender-ready leads found. Run pipeline first.")
        return

    all_leads = pd.concat(dfs, ignore_index=True)

    # Pick best email per unique company+address combo
    all_leads['dedup_key'] = (
        all_leads['contractor_domain'].fillna('') + '|' +
        all_leads.get('project_address_x', all_leads.get('project_address_y', pd.Series([''] * len(all_leads)))).fillna('')
    )

    def pick_best_email(group):
        for prefix in ['estimating@', 'projects@', 'info@', 'contact@', 'owner@']:
            match = group[group['email_candidate'].str.startswith(prefix, na=False)]
            if not match.empty:
                return match.iloc[0]
        return group.iloc[0]

    best_leads = (
        all_leads.groupby('dedup_key', group_keys=False)
        .apply(pick_best_email, include_groups=False)
        .reset_index(drop=True)
    )

    # Filter to unique domains only (one email per company max)
    best_leads = best_leads.drop_duplicates(subset='contractor_domain')

    print(f"\nGenerating emails for {len(best_leads)} unique companies...\n")
    results = []

    for _, row in best_leads.iterrows():
        company  = get_company_name(row)
        address  = str(row.get('project_address_x', row.get('project_address_y', '')) or '').strip()
        city     = str(row.get('project_city', '') or '').strip()
        state    = str(row.get('project_state', '') or '').strip()
        triggers = str(row.get('score_hits', '') or '').strip()
        score    = row.get('lift_probability_score', 5)
        email    = str(row.get('email_candidate', '') or '').strip()
        domain   = str(row.get('contractor_domain', '') or '').strip()
        tier     = str(row.get('tier', 'warm'))

        if not email or not address:
            continue

        print(f"  Writing → {company} | {address} | {email}")

        try:
            raw = write_email(company, address, city, state, triggers, score)
            subject, body = parse_email(raw)
            status = 'ready' if subject else 'error'
        except Exception as e:
            subject, body, status = '', f'ERROR: {e}', 'error'

        results.append({
            'to_email':        email,
            'subject':         subject,
            'body':            body,
            'company':         company,
            'project_address': address,
            'city':            city,
            'state':           state,
            'score':           score,
            'tier':            tier,
            'domain':          domain,
            'ready_to_send':   status,
            'generated_at':    datetime.now().isoformat(),
        })

        print(f"    Subject: {subject}")
        print(f"    Preview: {body[:120]}...")
        print()

    out_df = pd.DataFrame(results)
    out_df.to_csv(out_path, index=False)
    print(f"\n✓ {len(out_df)} emails saved → {out_path}")
    print(f"  Ready: {(out_df['ready_to_send']=='ready').sum()}")
    print(f"  Hot: {(out_df['tier']=='hot').sum()} | Warm: {(out_df['tier']=='warm').sum()} | Catchall: {(out_df['tier']=='catchall').sum()}")
    return out_df

if __name__ == "__main__":
    run_email_writer()
