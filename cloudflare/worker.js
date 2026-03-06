export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders(),
      });
    }

    if (request.method === "GET" && url.pathname === "/health") {
      return json({ ok: true, service: "cranegenius-ai-proxy" }, 200);
    }

    if (request.method !== "POST") {
      return json({ error: "Method not allowed", hint: "Use POST / for chat or POST /alert for SMS fallback" }, 405);
    }

    let payload;
    try {
      payload = await request.json();
    } catch {
      return json({ error: "Invalid JSON body" }, 400);
    }

    if (url.pathname === "/alert") {
      return await handleTwilioAlert(payload, env);
    }

    if (!env.ANTHROPIC_API_KEY) {
      return json({ error: "Worker secret ANTHROPIC_API_KEY is missing" }, 500);
    }

    const upstreamBody = {
      model: payload.model || "claude-sonnet-4-20250514",
      max_tokens: payload.max_tokens || 400,
      system: payload.system || "",
      messages: payload.messages || [],
    };

    const upstream = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify(upstreamBody),
    });

    const text = await upstream.text();
    return new Response(text, {
      status: upstream.status,
      headers: {
        "content-type": "application/json",
        ...corsHeaders(),
      },
    });
  },
};

function corsHeaders() {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET, POST, OPTIONS",
    "access-control-allow-headers": "content-type",
  };
}

async function handleTwilioAlert(payload, env) {
  const sid = env.TWILIO_ACCOUNT_SID;
  const token = env.TWILIO_AUTH_TOKEN;
  const from = env.TWILIO_FROM_NUMBER;
  const to = env.TWILIO_TO_NUMBER;

  if (!sid || !token || !from || !to) {
    return json({ error: "Twilio secrets missing", required: ["TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN", "TWILIO_FROM_NUMBER", "TWILIO_TO_NUMBER"] }, 500);
  }

  const source = (payload.source || "unknown").slice(0, 80);
  const userMsg = (payload.user_message || "").replace(/\s+/g, " ").slice(0, 260);
  const aiReply = (payload.ai_response || "").replace(/\s+/g, " ").slice(0, 180);
  const page = (payload.page_url || "").slice(0, 240);

  const smsBody =
    `CraneGenius AI fallback\n` +
    `Source: ${source}\n` +
    `User: ${userMsg}\n` +
    `AI: ${aiReply}\n` +
    `Page: ${page}`;

  const twilioBody = new URLSearchParams({
    To: to,
    From: from,
    Body: smsBody,
  });

  const auth = btoa(`${sid}:${token}`);
  const twilioRes = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${sid}/Messages.json`, {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
      authorization: `Basic ${auth}`,
    },
    body: twilioBody.toString(),
  });

  const twilioText = await twilioRes.text();
  if (!twilioRes.ok) {
    return json({ error: "Twilio send failed", status: twilioRes.status, details: twilioText.slice(0, 300) }, 502);
  }

  return json({ ok: true }, 200);
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: {
      "content-type": "application/json",
      ...corsHeaders(),
    },
  });
}
