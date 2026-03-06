## Cloudflare Worker AI Proxy Setup

This proxy keeps your Anthropic API key off the public website.

### 1) Deploy Worker

1. Install Wrangler (one-time):
   - `npm i -g wrangler`
2. Login:
   - `wrangler login`
3. Deploy from this folder:
   - `cd cloudflare`
   - `wrangler deploy`

### 2) Add Worker Secret

Run:

`wrangler secret put ANTHROPIC_API_KEY`

Paste your Anthropic API key when prompted.

### 2b) Optional: Twilio SMS fallback alerts

Add these Worker secrets if you want automatic SMS when AI fails:

- `wrangler secret put TWILIO_ACCOUNT_SID`
- `wrangler secret put TWILIO_AUTH_TOKEN`
- `wrangler secret put TWILIO_FROM_NUMBER` (Twilio number, e.g. `+15035550123`)
- `wrangler secret put TWILIO_TO_NUMBER` (your phone, e.g. `+15037734659`)

The frontend sends fallback payloads to `POST /alert` on the same Worker URL.

### 3) Configure Website

1. Copy `config.example.js` to `config.js` in repo root.
2. Set:
   - `window.__CG_PROXY_URL__ = "https://<your-worker>.workers.dev";`

`config.js` is already in `.gitignore`, so it stays private/local.

### 4) Publish Website

Commit and push your HTML changes.

The site will call your Worker URL for bot responses.

### Notes

- If `window.__CG_PROXY_URL__` is not set, bots show a live-support fallback message.
- CORS is open for browser calls (`GET, POST, OPTIONS`).
- For tighter security later, restrict `access-control-allow-origin` to your domain.
- Health check: `GET /health` returns `{ ok: true }`.
