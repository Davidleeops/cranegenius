from __future__ import annotations
import json, os, time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import jsonschema, requests

class LLMError(Exception): pass
class TransientLLMError(LLMError): pass
class PermanentLLMError(LLMError): pass

@dataclass
class LLMResult:
    provider: str
    model: str
    parsed: Dict[str, Any]
    raw_text: str

def _is_transient(status_code: int) -> bool:
    return status_code in (408, 429, 500, 502, 503, 504)

def _sleep_backoff(attempt: int) -> None:
    delay = min(2 ** attempt, 8)
    print(f"  [router] Backing off {delay}s...")
    time.sleep(delay)

def _validate_schema(data, schema): jsonschema.validate(instance=data, schema=schema)

def _extract_json_strict(text: str) -> Dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    return json.loads(text.strip())

def _repair_json(provider_call, task_name, schema, messages, bad_output):
    repair_messages = messages + [{"role": "user", "content": f"Your last response was not valid JSON.\nReturn ONLY valid JSON.\nTask: {task_name}\nSchema: {json.dumps(schema)}\nBad output: {bad_output[:500]}"}]
    _, text = provider_call(repair_messages)
    return text

def _call_anthropic(messages: List[Dict[str, str]]) -> Tuple[str, str]:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key: raise PermanentLLMError("Missing ANTHROPIC_API_KEY")
    model = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
    system_parts, msg_payload = [], []
    for m in messages:
        if m["role"] == "system": system_parts.append(m["content"])
        else: msg_payload.append({"role": m["role"], "content": m["content"]})
    payload = {"model": model, "max_tokens": int(os.environ.get("LLM_MAX_TOKENS", "2000")), "temperature": float(os.environ.get("LLM_TEMPERATURE", "0")), "system": "\n\n".join(system_parts), "messages": msg_payload}
    r = requests.post("https://api.anthropic.com/v1/messages", headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"}, json=payload, timeout=60)
    if not r.ok:
        if _is_transient(r.status_code): raise TransientLLMError(f"Anthropic {r.status_code}")
        raise PermanentLLMError(f"Anthropic {r.status_code}: {r.text[:200]}")
    text = "".join(b.get("text","") for b in r.json().get("content",[]) if b.get("type")=="text")
    return model, text.strip()

def _call_openai(messages: List[Dict[str, str]]) -> Tuple[str, str]:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key: raise PermanentLLMError("Missing OPENAI_API_KEY")
    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    payload = {"model": model, "messages": messages, "temperature": float(os.environ.get("LLM_TEMPERATURE","0")), "max_tokens": int(os.environ.get("LLM_MAX_TOKENS","2000"))}
    r = requests.post("https://api.openai.com/v1/chat/completions", headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}, json=payload, timeout=60)
    if not r.ok:
        if _is_transient(r.status_code): raise TransientLLMError(f"OpenAI {r.status_code}")
        raise PermanentLLMError(f"OpenAI {r.status_code}: {r.text[:200]}")
    return model, r.json()["choices"][0]["message"]["content"].strip()

PROVIDERS = [("anthropic", _call_anthropic), ("openai", _call_openai)]

def generate_json(task_name: str, schema: Dict[str, Any], messages: List[Dict[str, str]], max_retries: int = 3) -> LLMResult:
    last_error: Optional[Exception] = None
    for provider_name, provider_call in PROVIDERS:
        print(f"  [router] Trying {provider_name} for '{task_name}'...")
        for attempt in range(max_retries):
            try:
                model, text = provider_call(messages)
                try:
                    parsed = _extract_json_strict(text)
                    _validate_schema(parsed, schema)
                    print(f"  [router] ✓ {provider_name} succeeded")
                    return LLMResult(provider=provider_name, model=model, parsed=parsed, raw_text=text)
                except Exception:
                    repaired = _repair_json(provider_call, task_name, schema, messages, text)
                    parsed = _extract_json_strict(repaired)
                    _validate_schema(parsed, schema)
                    print(f"  [router] ✓ {provider_name} succeeded after repair")
                    return LLMResult(provider=provider_name, model=model, parsed=parsed, raw_text=repaired)
            except TransientLLMError as e:
                last_error = e
                if attempt < max_retries - 1: _sleep_backoff(attempt)
            except (PermanentLLMError, Exception) as e:
                last_error = e
                break
    raise LLMError(f"All providers failed for '{task_name}'. Last: {last_error}")
