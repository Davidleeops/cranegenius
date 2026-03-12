(function () {
  "use strict";

  var REGISTRY_PATH = "/config/page_context_registry.json";
  var FALLBACK_REPLY = "I can help, but live AI is unavailable right now. Call 1 (503) 773-4659 or email info@cranegenius.com.";

  function normalizePath(path) {
    var clean = String(path || "/").split("?")[0].split("#")[0].trim();
    if (!clean) return "/";
    if (!clean.startsWith("/")) clean = "/" + clean;
    if (clean !== "/" && !clean.endsWith("/")) clean += "/";
    return clean;
  }

  function safeArray(value) {
    return Array.isArray(value) ? value : [];
  }

  function escapeHtml(text) {
    return String(text || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function buildContextBlock(ctx) {
    if (!ctx || typeof ctx !== "object") return "";
    var lines = [];
    lines.push("PAGE CONTEXT:");
    lines.push("- path: " + String(ctx.page_path || ""));
    lines.push("- page_type: " + String(ctx.page_type || ""));
    lines.push("- title: " + String(ctx.title || ""));
    lines.push("- summary: " + String(ctx.summary || ""));
    lines.push("- manufacturers: " + safeArray(ctx.manufacturers).join(", "));
    lines.push("- represented_businesses: " + safeArray(ctx.represented_businesses).join(", "));
    lines.push("- products_or_services: " + safeArray(ctx.products_or_services).join(", "));
    lines.push("- key_talking_points: " + safeArray(ctx.key_talking_points).join(" | "));
    lines.push("- faq_context: " + safeArray(ctx.faq_context).join(" | "));
    if (ctx.cta_context && typeof ctx.cta_context === "object") {
      lines.push("- cta_primary: " + String(ctx.cta_context.primary || ""));
      lines.push("- cta_secondary: " + String(ctx.cta_context.secondary || ""));
      lines.push("- cta_quick_actions: " + safeArray(ctx.cta_context.quick_actions).join(" | "));
    }
    if (ctx.bot_instructions) {
      lines.push("- bot_instructions: " + String(ctx.bot_instructions));
    }
    return lines.join("\n");
  }

  async function sendViaProxy(payload) {
    var proxy = window.__CG_PROXY_URL__ || "";
    if (!proxy) {
      return { ok: false, reply: FALLBACK_REPLY, reason: "missing_proxy" };
    }
    try {
      var res = await fetch(proxy, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      var data = await res.json();
      if (!res.ok) {
        return {
          ok: false,
          reply: "AI connection issue. " + FALLBACK_REPLY,
          reason: (data && data.error && data.error.message) ? data.error.message : String(res.status)
        };
      }
      var text = (data && data.content && data.content[0] && data.content[0].text) ? data.content[0].text : "";
      if (!text) {
        return { ok: false, reply: FALLBACK_REPLY, reason: "empty_response" };
      }
      return { ok: true, reply: text };
    } catch (err) {
      return { ok: false, reply: FALLBACK_REPLY, reason: String(err || "network_error") };
    }
  }

  function installDefaultWidget(ctx, api) {
    if (document.getElementById("cg-page-bot")) return;
    if (document.getElementById("ai-panel") || document.getElementById("botPanel")) return;

    var quick = (ctx && ctx.cta_context && Array.isArray(ctx.cta_context.quick_actions) && ctx.cta_context.quick_actions.length)
      ? ctx.cta_context.quick_actions.slice(0, 3)
      : ["Get help", "Ask a question", "Talk to specialist"];

    var shell = document.createElement("div");
    shell.id = "cg-page-bot";
    shell.innerHTML = [
      '<button id="cg-bot-toggle" type="button" style="position:fixed;bottom:18px;right:18px;z-index:9999;background:#c9a84c;color:#0b1020;border:none;border-radius:999px;padding:12px 14px;font:700 12px/1 sans-serif;cursor:pointer;box-shadow:0 10px 24px rgba(0,0,0,.35)">AI</button>',
      '<div id="cg-bot-chat" style="display:none;position:fixed;bottom:68px;right:18px;z-index:9999;width:min(360px,calc(100vw - 24px));background:#0f1729;color:#e7eef9;border:1px solid rgba(201,168,76,.35);border-radius:10px;overflow:hidden;box-shadow:0 20px 40px rgba(0,0,0,.45)">',
      '<div style="padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.1);font:700 12px/1.3 sans-serif">CraneGenius AI</div>',
      '<div id="cg-bot-messages" style="max-height:260px;overflow:auto;padding:12px;font:13px/1.45 sans-serif"></div>',
      '<div id="cg-bot-quick" style="padding:0 12px 10px"></div>',
      '<div style="display:flex;gap:8px;padding:10px;border-top:1px solid rgba(255,255,255,.1)">',
      '<input id="cg-bot-input" type="text" placeholder="Ask a question..." style="flex:1;min-width:0;border:1px solid rgba(255,255,255,.18);background:#121d34;color:#e7eef9;border-radius:6px;padding:8px 10px">',
      '<button id="cg-bot-send" type="button" style="border:none;background:#c9a84c;color:#0b1020;border-radius:6px;padding:8px 10px;font:700 12px/1 sans-serif;cursor:pointer">Send</button>',
      '</div>',
      '</div>'
    ].join("");
    document.body.appendChild(shell);

    var isOpen = false;
    var history = [];
    var waiting = false;
    var toggleBtn = document.getElementById("cg-bot-toggle");
    var panel = document.getElementById("cg-bot-chat");
    var input = document.getElementById("cg-bot-input");
    var sendBtn = document.getElementById("cg-bot-send");
    var msgWrap = document.getElementById("cg-bot-messages");
    var quickWrap = document.getElementById("cg-bot-quick");

    function addMessage(role, text) {
      var line = document.createElement("div");
      var bg = role === "user" ? "rgba(201,168,76,.16)" : "rgba(255,255,255,.07)";
      line.style.cssText = "margin-bottom:8px;padding:8px 10px;border-radius:7px;background:" + bg + ";";
      line.innerHTML = escapeHtml(text);
      msgWrap.appendChild(line);
      msgWrap.scrollTop = msgWrap.scrollHeight;
    }

    function setQuickButtons() {
      quickWrap.innerHTML = quick.map(function (q) {
        return '<button type="button" class="cg-q" style="margin:0 6px 6px 0;border:1px solid rgba(255,255,255,.2);background:#15213b;color:#d9e2f0;border-radius:999px;padding:5px 9px;font:12px/1.2 sans-serif;cursor:pointer">' + escapeHtml(q) + '</button>';
      }).join("");
      quickWrap.querySelectorAll(".cg-q").forEach(function (btn, idx) {
        btn.addEventListener("click", function () {
          input.value = quick[idx];
          sendMessage();
        });
      });
    }

    async function sendMessage() {
      if (waiting) return;
      var msg = (input.value || "").trim();
      if (!msg) return;
      waiting = true;
      input.value = "";
      addMessage("user", msg);
      var out = await api.sendMessage({
        message: msg,
        history: history.slice(-8),
        systemPrompt: ""
      });
      history.push({ role: "user", content: msg });
      history.push({ role: "assistant", content: out.reply });
      addMessage("assistant", out.reply);
      waiting = false;
    }

    toggleBtn.addEventListener("click", function () {
      isOpen = !isOpen;
      panel.style.display = isOpen ? "block" : "none";
      if (isOpen) input.focus();
    });
    sendBtn.addEventListener("click", sendMessage);
    input.addEventListener("keydown", function (e) {
      if (e.key === "Enter") {
        e.preventDefault();
        sendMessage();
      }
    });

    var welcome = "Hi. I can help with this page.";
    if (ctx && ctx.summary) {
      welcome = "Hi. " + ctx.summary;
    }
    addMessage("assistant", welcome);
    history.push({ role: "assistant", content: welcome });
    setQuickButtons();
  }

  async function init() {
    var normalizedPath = normalizePath(window.location.pathname || "/");
    var registry = { pages: [], required_fields: [] };
    try {
      var res = await fetch(REGISTRY_PATH, { cache: "no-store" });
      if (res.ok) registry = await res.json();
    } catch (_err) {
      registry = { pages: [], required_fields: [] };
    }

    var pages = Array.isArray(registry.pages) ? registry.pages : [];
    var currentContext = pages.find(function (p) {
      return normalizePath(p && p.page_path) === normalizedPath;
    }) || null;

    window.CG_PAGE_CONTEXT = currentContext;

    var api = {
      getCurrentPath: function () { return normalizedPath; },
      getCurrentContext: function () { return currentContext; },
      getRegistry: function () { return registry; },
      formatSystemPrompt: function (basePrompt) {
        var base = String(basePrompt || "You are CraneGenius AI. Keep responses concise, practical, and page-aware.");
        var ctxBlock = buildContextBlock(currentContext);
        return ctxBlock ? (base + "\n\n" + ctxBlock) : base;
      },
      sendMessage: async function (args) {
        args = args || {};
        var userMessage = String(args.message || "").trim();
        var history = Array.isArray(args.history) ? args.history : [];
        var systemPrompt = api.formatSystemPrompt(args.systemPrompt);
        var payload = {
          model: "claude-sonnet-4-20250514",
          max_tokens: 450,
          system: systemPrompt,
          messages: history.concat([{ role: "user", content: userMessage }]).slice(-12)
        };
        return sendViaProxy(payload);
      },
      openAndSend: function (message) {
        var msg = String(message || "").trim();
        if (!msg) return false;
        var toggle = document.getElementById("cg-bot-toggle");
        var input = document.getElementById("cg-bot-input");
        var send = document.getElementById("cg-bot-send");
        var panel = document.getElementById("cg-bot-chat");
        if (!toggle || !input || !send) return false;
        if (panel && panel.style.display !== "block") toggle.click();
        input.value = msg;
        send.click();
        return true;
      }
    };

    window.CGPageBot = api;
    installDefaultWidget(currentContext, api);
  }

  init();
})();
