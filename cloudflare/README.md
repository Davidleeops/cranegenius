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
- CORS is open for browser calls (`POST, OPTIONS`).
- For tighter security later, restrict `access-control-allow-origin` to your domain.

