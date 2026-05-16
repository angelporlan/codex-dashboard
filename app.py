from __future__ import annotations

import json
import os
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


APP_DIR = Path(__file__).resolve().parent
CODEX_DIR = Path.home() / ".codex"
STATE_DB = CODEX_DIR / "state_5.sqlite"
LOGS_DB = CODEX_DIR / "logs_2.sqlite"
CONFIG_FILE = APP_DIR / "dashboard_config.json"

TOKEN_KEYS = (
    "input_token_count",
    "output_token_count",
    "cached_token_count",
    "reasoning_token_count",
    "tool_token_count",
)


def read_json(path: Path, default: dict) -> dict:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def open_ro(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def epoch_to_iso(value: int | float | None) -> str | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
    except (OSError, OverflowError, ValueError):
        return None


def iso_to_epoch(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def project_from_cwd(value: str | None) -> str:
    clean = (value or "").replace("\\\\?\\", "").strip()
    if not clean:
        return "(sin proyecto)"
    normalized = clean.replace("\\", "/").rstrip("/")
    name = normalized.split("/")[-1].strip()
    return name or normalized or "(sin proyecto)"


def parse_kv_metrics(message: str) -> dict:
    metrics = {}
    for key in TOKEN_KEYS:
        match = re.search(rf"\b{re.escape(key)}=(\d+)", message)
        metrics[key] = int(match.group(1)) if match else 0

    for key in ("conversation.id", "model", "slug", "event.timestamp"):
        match = re.search(rf'\b{re.escape(key)}=("[^"]+"|\S+)', message)
        if match:
            metrics[key.replace(".", "_")] = match.group(1).strip('"')
    return metrics


def parse_rate_limit_event(message: str) -> dict | None:
    marker = "websocket event: "
    index = message.find(marker)
    if index < 0:
        return None
    payload = message[index + len(marker) :].strip()
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return None
    if data.get("type") != "codex.rate_limits":
        return None
    return data


def normalize_session_rate_limits(payload: dict, timestamp: str | None, path: Path | None = None) -> dict | None:
    rate_limits = payload.get("rate_limits")
    if not rate_limits:
        return None
    now = int(datetime.now(tz=timezone.utc).timestamp())
    normalized = {
        "type": "codex.rate_limits",
        "plan_type": rate_limits.get("plan_type"),
        "rate_limits": {},
        "code_review_rate_limits": None,
        "additional_rate_limits": None,
        "credits": rate_limits.get("credits"),
        "promo": None,
        "seen_at": timestamp,
        "source": "session_jsonl",
    }
    if path:
        normalized["source_file"] = path.name
    for name in ("primary", "secondary"):
        item = rate_limits.get(name) or {}
        reset_at = item.get("reset_at", item.get("resets_at"))
        normalized["rate_limits"][name] = {
            "used_percent": item.get("used_percent", 0),
            "window_minutes": item.get("window_minutes"),
            "reset_at": reset_at,
            "reset_after_seconds": max(0, int(reset_at or now) - now),
        }
    return normalized


def load_threads() -> dict:
    if not STATE_DB.exists():
        return {"threads": [], "totals": {"tokens": 0, "count": 0}, "by_model": []}

    with open_ro(STATE_DB) as con:
        rows = con.execute(
            """
            select id, title, cwd, model, reasoning_effort, tokens_used,
                   created_at, updated_at, archived, source, first_user_message,
                   git_branch, git_sha, git_origin_url, cli_version,
                   agent_nickname, agent_role
            from threads
            order by updated_at desc
            """
        ).fetchall()

    threads = []
    for row in rows:
        cwd = (row["cwd"] or "").replace("\\\\?\\", "")
        threads.append(
            {
                "id": row["id"],
                "title": row["title"] or "(sin titulo)",
                "cwd": cwd,
                "project": project_from_cwd(cwd),
                "model": row["model"] or "desconocido",
                "reasoning_effort": row["reasoning_effort"] or "",
                "tokens_used": row["tokens_used"] or 0,
                "created_at": epoch_to_iso(row["created_at"]),
                "updated_at": epoch_to_iso(row["updated_at"]),
                "archived": bool(row["archived"]),
                "source": row["source"],
                "first_user_message": row["first_user_message"] or "",
                "git_branch": row["git_branch"] or "",
                "git_sha": row["git_sha"] or "",
                "git_origin_url": row["git_origin_url"] or "",
                "cli_version": row["cli_version"] or "",
                "agent_nickname": row["agent_nickname"] or "",
                "agent_role": row["agent_role"] or "",
            }
        )

    by_model = defaultdict(lambda: {"thread_tokens": 0, "threads": 0})
    for item in threads:
        model = item["model"] or "desconocido"
        by_model[model]["thread_tokens"] += item["tokens_used"]
        by_model[model]["threads"] += 1

    return {
        "threads": threads,
        "totals": {
            "tokens": sum(item["tokens_used"] for item in threads),
            "count": len(threads),
        },
        "by_model": [
            {"model": key, **value}
            for key, value in sorted(by_model.items(), key=lambda item: item[1]["thread_tokens"], reverse=True)
        ],
    }


def load_response_events(days: int) -> dict:
    if not LOGS_DB.exists():
        return {"events": [], "by_day": [], "by_model": []}

    now = int(datetime.now(tz=timezone.utc).timestamp())
    since = now - max(days, 1) * 24 * 60 * 60

    with open_ro(LOGS_DB) as con:
        rows = con.execute(
            """
            select id, ts, feedback_log_body
            from logs
            where ts >= ?
              and target = 'codex_otel.trace_safe'
              and feedback_log_body like '%event.kind=response.completed%'
              and feedback_log_body like '%input_token_count=%'
            order by ts asc, id asc
            """,
            (since,),
        ).fetchall()

    events = []
    seen = set()
    for row in rows:
        metrics = parse_kv_metrics(row["feedback_log_body"] or "")
        dedupe_key = (
            metrics.get("conversation_id"),
            metrics.get("event_timestamp"),
            metrics.get("input_token_count"),
            metrics.get("output_token_count"),
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        total = metrics["input_token_count"] + metrics["output_token_count"]
        event = {
            "id": row["id"],
            "ts": row["ts"],
            "date": datetime.fromtimestamp(row["ts"], tz=timezone.utc).strftime("%Y-%m-%d"),
            "timestamp": metrics.get("event_timestamp") or epoch_to_iso(row["ts"]),
            "thread_id": metrics.get("conversation_id"),
            "model": metrics.get("model") or metrics.get("slug") or "desconocido",
            "input": metrics["input_token_count"],
            "output": metrics["output_token_count"],
            "cached": metrics["cached_token_count"],
            "reasoning": metrics["reasoning_token_count"],
            "tool": metrics["tool_token_count"],
            "total": total,
        }
        events.append(event)

    by_day = defaultdict(lambda: {"input": 0, "output": 0, "cached": 0, "reasoning": 0, "total": 0, "calls": 0})
    by_model = defaultdict(lambda: {"input": 0, "output": 0, "cached": 0, "reasoning": 0, "total": 0, "calls": 0})
    for event in events:
        for bucket in (by_day[event["date"]], by_model[event["model"]]):
            bucket["input"] += event["input"]
            bucket["output"] += event["output"]
            bucket["cached"] += event["cached"]
            bucket["reasoning"] += event["reasoning"]
            bucket["total"] += event["total"]
            bucket["calls"] += 1

    return {
        "events": list(reversed(events[-250:])),
        "by_day": [{"date": key, **value} for key, value in sorted(by_day.items())],
        "by_model": [{"model": key, **value} for key, value in sorted(by_model.items(), key=lambda item: item[1]["total"], reverse=True)],
    }


def load_latest_rate_limits() -> dict | None:
    candidates = []

    if LOGS_DB.exists():
        with open_ro(LOGS_DB) as con:
            rows = con.execute(
                """
                select ts, feedback_log_body
                from logs
                where feedback_log_body like '%websocket event: {"type":"codex.rate_limits"%'
                order by id desc
                limit 20
                """
            ).fetchall()

        for row in rows:
            event = parse_rate_limit_event(row["feedback_log_body"] or "")
            if event:
                event["seen_at"] = epoch_to_iso(row["ts"])
                event["source"] = "logs_sqlite"
                candidates.append(event)
                break

    candidates.extend(load_latest_session_rate_limits())
    if not candidates:
        return None

    return max(candidates, key=lambda item: iso_to_epoch(item.get("seen_at")) or 0)


def load_latest_session_rate_limits() -> list[dict]:
    if not STATE_DB.exists():
        return []

    with open_ro(STATE_DB) as con:
        rows = con.execute(
            """
            select rollout_path
            from threads
            where rollout_path is not null and rollout_path != ''
            order by updated_at desc
            limit 30
            """
        ).fetchall()

    candidates = []
    seen_paths = set()
    for row in rows:
        path = Path(row["rollout_path"])
        if path in seen_paths or not path.exists():
            continue
        seen_paths.add(path)
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        for line in reversed(lines):
            if '"rate_limits"' not in line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            payload = data.get("payload") or {}
            event = normalize_session_rate_limits(payload, data.get("timestamp"), path)
            if event:
                candidates.append(event)
                break
    return candidates


def build_summary(days: int) -> dict:
    config = read_json(CONFIG_FILE, {"token_budget": {"monthly": 0, "daily": 0}, "notes": ""})
    threads = load_threads()
    responses = load_response_events(days)
    rate_limits = load_latest_rate_limits()

    totals = {"input": 0, "output": 0, "cached": 0, "reasoning": 0, "total": 0, "calls": 0}
    for row in responses["by_day"]:
        for key in totals:
            totals[key] += row[key]

    window_by_model = {row["model"]: row for row in responses["by_model"]}
    model_names = {row["model"] for row in threads["by_model"]} | set(window_by_model)
    model_usage = []
    thread_by_model = {row["model"]: row for row in threads["by_model"]}
    for model in sorted(
        model_names,
        key=lambda name: (
            thread_by_model.get(name, {}).get("thread_tokens", 0),
            window_by_model.get(name, {}).get("total", 0),
        ),
        reverse=True,
    ):
        thread_row = thread_by_model.get(model, {})
        window_row = window_by_model.get(model, {})
        model_usage.append(
            {
                "model": model,
                "threads": thread_row.get("threads", 0),
                "thread_tokens": thread_row.get("thread_tokens", 0),
                "window_calls": window_row.get("calls", 0),
                "window_tokens": window_row.get("total", 0),
                "window_input": window_row.get("input", 0),
                "window_output": window_row.get("output", 0),
                "window_reasoning": window_row.get("reasoning", 0),
            }
        )

    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "paths": {
            "state_db": str(STATE_DB.name),
            "logs_db": str(LOGS_DB.name),
            "config": str(CONFIG_FILE.name),
        },
        "window_days": days,
        "config": config,
        "rate_limits": rate_limits,
        "response_totals": totals,
        "responses": responses,
        "model_usage": model_usage,
        "thread_totals": threads["totals"],
        "threads": threads["threads"],
    }


HTML = r"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Codex Usage Dashboard</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f4f7fb;
      --bg-alt: #eef3fb;
      --ambient-1: rgba(37, 99, 235, .14);
      --ambient-2: rgba(15, 159, 110, .10);
      --panel: rgba(255, 255, 255, .86);
      --panel-solid: #ffffff;
      --control: rgba(255, 255, 255, .94);
      --control-hover: #f8fafc;
      --ink: #101828;
      --muted: #667085;
      --line: rgba(16, 24, 40, .12);
      --line-strong: rgba(16, 24, 40, .18);
      --blue: #2563eb;
      --blue-soft: rgba(37, 99, 235, .11);
      --green: #0f9f6e;
      --green-soft: rgba(15, 159, 110, .12);
      --amber: #d97706;
      --amber-soft: rgba(217, 119, 6, .14);
      --red: #dc2626;
      --red-soft: rgba(220, 38, 38, .12);
      --violet: #7c3aed;
      --violet-soft: rgba(124, 58, 237, .12);
      --shadow: 0 18px 48px rgba(17, 24, 39, .10);
      --chart-bg: rgba(255, 255, 255, .88);
      --row-hover: rgba(37, 99, 235, .06);
      --focus: 0 0 0 4px rgba(37, 99, 235, .17);
    }
    body[data-theme="dark"] {
      color-scheme: dark;
      --bg: #080c14;
      --bg-alt: #111827;
      --ambient-1: rgba(59, 130, 246, .16);
      --ambient-2: rgba(20, 184, 166, .12);
      --panel: rgba(15, 23, 42, .76);
      --panel-solid: #0f172a;
      --control: rgba(15, 23, 42, .82);
      --control-hover: rgba(30, 41, 59, .92);
      --ink: #e5e7eb;
      --muted: #9ca3af;
      --line: rgba(148, 163, 184, .20);
      --line-strong: rgba(148, 163, 184, .28);
      --blue: #60a5fa;
      --blue-soft: rgba(96, 165, 250, .16);
      --green: #34d399;
      --green-soft: rgba(52, 211, 153, .13);
      --amber: #fbbf24;
      --amber-soft: rgba(251, 191, 36, .14);
      --red: #f87171;
      --red-soft: rgba(248, 113, 113, .14);
      --violet: #a78bfa;
      --violet-soft: rgba(167, 139, 250, .15);
      --shadow: 0 22px 60px rgba(0, 0, 0, .34);
      --chart-bg: rgba(15, 23, 42, .76);
      --row-hover: rgba(96, 165, 250, .10);
      --focus: 0 0 0 4px rgba(96, 165, 250, .18);
    }
    * { box-sizing: border-box; }
    html { min-height: 100%; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--ink);
      background:
        radial-gradient(circle at 12% 8%, var(--ambient-1), transparent 34rem),
        radial-gradient(circle at 86% 4%, var(--ambient-2), transparent 32rem),
        linear-gradient(135deg, var(--bg), var(--bg-alt));
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      transition: background .28s ease, color .28s ease;
    }
    button, input, select { font: inherit; }
    button, select, input { color: var(--ink); }
    .shell { max-width: 1360px; margin: 0 auto; padding: 28px; }
    header {
      display: flex;
      justify-content: space-between;
      gap: 18px;
      align-items: flex-start;
      padding: 8px 0 24px;
    }
    h1 {
      font-size: clamp(32px, 5vw, 56px);
      line-height: .95;
      margin: 8px 0 12px;
      letter-spacing: -.055em;
    }
    p { margin: 0; color: var(--muted); }
    .eyebrow {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: var(--muted);
      font-size: 13px;
      font-weight: 800;
      letter-spacing: .08em;
      text-transform: uppercase;
    }
    .live-dot {
      width: 9px;
      height: 9px;
      border-radius: 999px;
      background: var(--green);
      box-shadow: 0 0 0 0 var(--green-soft);
      animation: pulseDot 2.4s ease-out infinite;
    }
    .controls {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
      align-items: center;
    }
    .control, button {
      min-height: 42px;
      border: 1px solid var(--line);
      background: var(--control);
      border-radius: 12px;
      padding: 9px 13px;
      color: var(--ink);
      box-shadow: 0 1px 0 rgba(255, 255, 255, .05) inset;
      transition: transform .18s ease, border-color .18s ease, background .18s ease, box-shadow .18s ease;
    }
    button { cursor: pointer; }
    button:hover, .control:hover { border-color: var(--blue); background: var(--control-hover); }
    button:hover { transform: translateY(-1px); }
    button:active { transform: translateY(0); }
    button:focus-visible, .control:focus-visible { outline: none; box-shadow: var(--focus); border-color: var(--blue); }
    .theme-button { display: inline-flex; align-items: center; gap: 8px; }
    .theme-glyph {
      display: inline-grid;
      place-items: center;
      width: 22px;
      height: 22px;
      border-radius: 999px;
      background: var(--blue-soft);
      color: var(--blue);
      font-weight: 900;
      line-height: 1;
    }
    .status-text { color: var(--muted); font-size: 13px; }
    .grid { display: grid; grid-template-columns: repeat(12, 1fr); gap: 16px; }
    .panel {
      min-width: 0;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      box-shadow: var(--shadow);
      padding: 18px;
      backdrop-filter: blur(18px);
      animation: panelIn .42s ease both;
      transition: transform .22s ease, box-shadow .22s ease, border-color .22s ease, background .22s ease;
    }
    .panel:hover { transform: translateY(-2px); border-color: var(--line-strong); }
    .grid > .panel:nth-child(2) { animation-delay: .03s; }
    .grid > .panel:nth-child(3) { animation-delay: .06s; }
    .grid > .panel:nth-child(4) { animation-delay: .09s; }
    .grid > .panel:nth-child(5) { animation-delay: .12s; }
    .grid > .panel:nth-child(6) { animation-delay: .15s; }
    .grid > .panel:nth-child(7) { animation-delay: .18s; }
    .grid > .panel:nth-child(8) { animation-delay: .21s; }
    .grid > .panel:nth-child(9) { animation-delay: .24s; }
    .metric-card { position: relative; padding-right: 54px; overflow: visible; }
    .metric-card::after {
      content: "";
      position: absolute;
      inset: auto 18px 14px 18px;
      height: 3px;
      border-radius: 999px;
      background: linear-gradient(90deg, var(--blue), transparent);
      opacity: .42;
    }
    .info {
      position: absolute;
      top: 14px;
      right: 14px;
      width: 30px;
      height: 30px;
      padding: 0;
      display: inline-grid;
      place-items: center;
      border-radius: 999px;
      color: var(--muted);
      cursor: help;
      z-index: 3;
    }
    .info:hover, .info:focus { color: var(--blue); }
    .info svg { width: 16px; height: 16px; stroke-width: 2.2; }
    .tip {
      position: absolute;
      z-index: 10;
      top: 42px;
      right: 0;
      width: min(282px, 72vw);
      padding: 11px 12px;
      border: 1px solid var(--line-strong);
      border-radius: 14px;
      background: var(--panel-solid);
      color: var(--ink);
      box-shadow: var(--shadow);
      font-size: 13px;
      line-height: 1.38;
      font-weight: 600;
      opacity: 0;
      visibility: hidden;
      transform: translateY(-6px) scale(.98);
      transition: opacity .16s ease, transform .16s ease, visibility .16s ease;
    }
    .info:hover .tip, .info:focus .tip { opacity: 1; visibility: visible; transform: translateY(0) scale(1); }
    .span-3 { grid-column: span 3; }
    .span-4 { grid-column: span 4; }
    .span-5 { grid-column: span 5; }
    .span-7 { grid-column: span 7; }
    .span-8 { grid-column: span 8; }
    .span-12 { grid-column: span 12; }
    .label {
      color: var(--muted);
      font-size: 12px;
      font-weight: 900;
      letter-spacing: .08em;
      text-transform: uppercase;
    }
    .metric {
      font-size: clamp(26px, 3vw, 36px);
      font-weight: 900;
      margin-top: 9px;
      letter-spacing: -.035em;
      font-variant-numeric: tabular-nums;
    }
    .sub, .path { color: var(--muted); font-size: 13px; margin-top: 6px; }
    .bar {
      height: 10px;
      border-radius: 999px;
      background: var(--blue-soft);
      overflow: hidden;
      margin-top: 12px;
      border: 1px solid var(--line);
    }
    .fill {
      height: 100%;
      width: 0%;
      border-radius: inherit;
      background: linear-gradient(90deg, var(--blue), var(--green));
      transition: width .55s cubic-bezier(.2, .8, .2, 1), background .18s ease;
    }
    .fill.warn { background: linear-gradient(90deg, var(--amber), var(--violet)); }
    .fill.danger { background: linear-gradient(90deg, var(--red), var(--amber)); }
    .row { display: flex; justify-content: space-between; gap: 12px; align-items: center; }
    .stack { display: grid; gap: 14px; }
    .section-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
      margin-bottom: 14px;
    }
    canvas {
      width: 100%;
      height: 270px;
      display: block;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: var(--chart-bg);
    }
    .legend { display: flex; gap: 12px; flex-wrap: wrap; margin-top: 12px; color: var(--muted); font-size: 13px; }
    .legend span { display: inline-flex; align-items: center; gap: 6px; }
    .swatch { width: 10px; height: 10px; border-radius: 999px; background: var(--blue); }
    .swatch.output { background: var(--green); }
    .table-wrap { overflow: auto; margin-top: 12px; border-radius: 16px; border: 1px solid var(--line); }
    table { width: 100%; border-collapse: collapse; table-layout: fixed; min-width: 760px; }
    th, td { padding: 12px 12px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }
    th {
      position: sticky;
      top: 0;
      z-index: 1;
      color: var(--muted);
      background: var(--panel-solid);
      font-size: 12px;
      letter-spacing: .06em;
      text-transform: uppercase;
    }
    td { font-size: 14px; }
    tbody tr { animation: rowIn .22s ease both; transition: background .16s ease; }
    tbody tr:hover { background: var(--row-hover); }
    tbody tr:last-child td { border-bottom: 0; }
    .truncate { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .num { text-align: right; font-variant-numeric: tabular-nums; }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 4px 9px;
      font-size: 12px;
      color: var(--muted);
      max-width: 100%;
      white-space: nowrap;
    }
    .pill.model { color: var(--blue); background: var(--blue-soft); border-color: transparent; }
    .pill.project { color: var(--violet); background: var(--violet-soft); border-color: transparent; }
    .pill.active { color: var(--green); background: var(--green-soft); border-color: transparent; }
    .pill.archived { color: var(--amber); background: var(--amber-soft); border-color: transparent; }
    .settings { display: flex; gap: 10px; flex-wrap: wrap; align-items: end; margin-top: 14px; }
    .field, .settings label {
      display: grid;
      gap: 6px;
      color: var(--muted);
      font-size: 13px;
      font-weight: 800;
    }
    .settings input { width: 160px; }
    .thread-toolbar {
      display: grid;
      grid-template-columns: 1.1fr 1.1fr 1.2fr .9fr .9fr 1.4fr .85fr;
      gap: 10px;
      margin-top: 14px;
      align-items: end;
    }
    .thread-toolbar .control { width: 100%; }
    .thread-row { cursor: pointer; }
    .thread-row:focus-visible { outline: none; box-shadow: var(--focus); }
    .drawer-backdrop {
      position: fixed;
      inset: 0;
      z-index: 40;
      display: none;
      background: rgba(15, 23, 42, .46);
      backdrop-filter: blur(10px);
      padding: 22px;
      overflow: auto;
    }
    .drawer-backdrop.open { display: grid; place-items: center; }
    .thread-dialog {
      width: min(860px, 100%);
      max-height: min(86vh, 900px);
      overflow: auto;
      background: var(--panel-solid);
      border: 1px solid var(--line-strong);
      border-radius: 22px;
      box-shadow: 0 28px 90px rgba(0, 0, 0, .28);
      padding: 22px;
      animation: panelIn .22s ease both;
    }
    .dialog-head {
      display: flex;
      justify-content: space-between;
      gap: 14px;
      align-items: flex-start;
      border-bottom: 1px solid var(--line);
      padding-bottom: 16px;
      margin-bottom: 16px;
    }
    .dialog-title {
      margin: 6px 0 10px;
      font-size: clamp(22px, 3vw, 34px);
      line-height: 1.05;
      letter-spacing: -.035em;
    }
    .dialog-close {
      width: 38px;
      height: 38px;
      padding: 0;
      display: inline-grid;
      place-items: center;
      border-radius: 999px;
      flex: 0 0 auto;
    }
    .detail-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-top: 14px;
    }
    .detail-item {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 12px;
      background: var(--control);
      min-width: 0;
    }
    .detail-value {
      margin-top: 5px;
      font-weight: 800;
      overflow-wrap: anywhere;
    }
    .detail-message {
      white-space: pre-wrap;
      line-height: 1.45;
      max-height: 220px;
      overflow: auto;
    }
    .empty { color: var(--muted); text-align: center; padding: 28px 12px; }
    .ok { color: var(--green); }
    .warnText { color: var(--amber); }
    .error { color: var(--red); }
    .loading { opacity: .72; pointer-events: none; }
    @keyframes panelIn {
      from { opacity: 0; transform: translateY(12px) scale(.992); }
      to { opacity: 1; transform: translateY(0) scale(1); }
    }
    @keyframes rowIn {
      from { opacity: 0; transform: translateY(5px); }
      to { opacity: 1; transform: translateY(0); }
    }
    @keyframes pulseDot {
      0% { box-shadow: 0 0 0 0 var(--green-soft); }
      70% { box-shadow: 0 0 0 10px transparent; }
      100% { box-shadow: 0 0 0 0 transparent; }
    }
    @media (max-width: 1120px) {
      .span-3, .span-4 { grid-column: span 6; }
      .span-5, .span-7, .span-8 { grid-column: span 12; }
      .thread-toolbar { grid-template-columns: repeat(3, minmax(160px, 1fr)); }
    }
    @media (max-width: 760px) {
      header { display: grid; }
      .controls { justify-content: start; }
      .span-3, .span-4, .span-5, .span-7, .span-8 { grid-column: span 12; }
      .thread-toolbar { grid-template-columns: 1fr; }
      .metric { font-size: 27px; }
      .shell { padding: 18px; }
      .section-head { display: grid; }
      .detail-grid { grid-template-columns: 1fr; }
      .drawer-backdrop { padding: 12px; }
    }
    @media (prefers-reduced-motion: reduce) {
      *, *::before, *::after { animation: none !important; transition: none !important; scroll-behavior: auto !important; }
    }
  </style>
</head>
<body>
  <main class="shell">
    <header>
      <div>
        <span class="eyebrow"><span class="live-dot" aria-hidden="true"></span> Local dashboard</span>
        <h1>Codex Usage</h1>
        <p>Tokens, sesiones y límites leídos desde tu instalación local de Codex.</p>
      </div>
      <div class="controls">
        <select id="days" class="control" aria-label="Ventana de días">
          <option value="7">7 días</option>
          <option value="30" selected>30 días</option>
          <option value="90">90 días</option>
          <option value="365">365 días</option>
        </select>
        <button id="themeToggle" class="theme-button" type="button" aria-pressed="false" title="Cambiar tema">
          <span id="themeIcon" class="theme-glyph" aria-hidden="true">☾</span><span id="themeLabel">Modo oscuro</span>
        </button>
        <button id="refresh" type="button" title="Actualizar">Actualizar</button>
        <span class="status-text" id="lastUpdated">sin actualizar</span>
      </div>
    </header>

    <section class="grid" aria-live="polite">
      <div class="panel span-3 metric-card">
        <button class="info" type="button" aria-label="Qué significa tokens en ventana">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true"><circle cx="12" cy="12" r="10"></circle><path d="M12 16v-4"></path><path d="M12 8h.01"></path></svg>
          <span class="tip">Suma de tokens de entrada y salida detectados en los eventos response.completed dentro de la ventana seleccionada.</span>
        </button>
        <div class="label">Tokens en ventana</div><div class="metric" id="totalTokens">-</div><div class="sub" id="totalTokensSub">input + output</div>
      </div>
      <div class="panel span-3 metric-card">
        <button class="info" type="button" aria-label="Qué significa llamadas medidas">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true"><circle cx="12" cy="12" r="10"></circle><path d="M12 16v-4"></path><path d="M12 8h.01"></path></svg>
          <span class="tip">Número de respuestas de Codex con métricas de tokens encontradas en los logs locales para esta ventana.</span>
        </button>
        <div class="label">Llamadas medidas</div><div class="metric" id="calls">-</div><div class="sub">Eventos response.completed</div>
      </div>
      <div class="panel span-3 metric-card">
        <button class="info" type="button" aria-label="Qué significa tokens por threads">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true"><circle cx="12" cy="12" r="10"></circle><path d="M12 16v-4"></path><path d="M12 8h.01"></path></svg>
          <span class="tip">Total acumulado que Codex guarda por conversación en state_5.sqlite. Sirve como histórico local amplio.</span>
        </button>
        <div class="label">Tokens por threads</div><div class="metric" id="threadTokens">-</div><div class="sub" id="threadCount">Estado local acumulado</div>
      </div>
      <div class="panel span-3 metric-card">
        <button class="info" type="button" aria-label="Qué significa caché">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true"><circle cx="12" cy="12" r="10"></circle><path d="M12 16v-4"></path><path d="M12 8h.01"></path></svg>
          <span class="tip">Tokens marcados como cacheados por Codex. Pueden reducir coste o carga real, pero no equivalen siempre a gasto facturable.</span>
        </button>
        <div class="label">Caché</div><div class="metric" id="cached">-</div><div class="sub">Tokens cacheados registrados</div>
      </div>

      <div class="panel span-5">
        <div class="section-head">
          <div>
            <div class="label" style="display: flex; align-items: center; gap: 6px;">
              Límites de Codex
              <button class="info" type="button" aria-label="Información sobre porcentajes" style="position: static; width: 16px; height: 16px;">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true"><circle cx="12" cy="12" r="10"></circle><path d="M12 16v-4"></path><path d="M12 8h.01"></path></svg>
                <span class="tip" style="top: auto; bottom: 100%; margin-bottom: 8px;">Por defecto se muestra el porcentaje consumido. La app oficial de Codex suele mostrar el porcentaje restante. Usa el botón para alternar.</span>
              </button>
            </div>
            <p id="plan">Buscando evento local reciente</p>
          </div>
          <div style="display: flex; gap: 8px; align-items: center;">
            <button id="toggleLimitsBtn" type="button" style="padding: 4px 10px; min-height: unset; font-size: 12px; border-radius: 999px;">Mostrar Restante</button>
            <span class="pill" id="limitSeen">-</span>
          </div>
        </div>
        <div class="stack" id="limits"></div>
      </div>

      <div class="panel span-7">
        <div class="section-head"><div><div class="label">Tendencia diaria</div><p>Uso input/output dentro de la ventana seleccionada.</p></div></div>
        <canvas id="dailyChart" width="900" height="270" aria-label="Gráfico diario de tokens"></canvas>
        <div class="legend"><span><i class="swatch"></i>Input total</span><span><i class="swatch output"></i>Output</span></div>
      </div>

      <div class="panel span-4">
        <div class="label">Presupuesto local</div>
        <p>Opcional: define tus propios topes de tokens para compararlos con el consumo.</p>
        <div class="settings">
          <label>Diario <input id="dailyBudget" class="control" type="number" min="0" step="1000"></label>
          <label>Mensual <input id="monthlyBudget" class="control" type="number" min="0" step="1000"></label>
          <button id="saveSettings" type="button">Guardar</button>
        </div>
        <div id="budgetBars" class="stack" style="margin-top: 14px;"></div>
      </div>

      <div class="panel span-8">
        <div class="label">Modelos</div>
        <p>Histórico local por threads y detalle medido en la ventana seleccionada.</p>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Modelo</th><th class="num">Threads</th><th class="num">Tokens históricos</th><th class="num">Llamadas ventana</th><th class="num">Tokens ventana</th><th>Input / Output</th></tr></thead>
            <tbody id="models"></tbody>
          </table>
        </div>
      </div>

      <div class="panel span-12">
        <div class="section-head">
          <div>
            <div class="label">Threads recientes</div>
            <p id="threadMeta">Filtra por proyecto, modelo, tokens o texto.</p>
          </div>
          <button id="clearThreadFilters" type="button">Limpiar filtros</button>
        </div>
        <div class="thread-toolbar">
          <label class="field">Proyecto
            <select id="threadProjectFilter" class="control" aria-label="Filtrar threads por proyecto"></select>
          </label>
          <label class="field">Modelo
            <select id="threadModelFilter" class="control" aria-label="Filtrar threads por modelo"></select>
          </label>
          <label class="field">Ordenar
            <select id="threadSort" class="control" aria-label="Ordenar threads">
              <option value="updated_desc">Más recientes</option>
              <option value="tokens_desc">Mayor gasto de tokens</option>
              <option value="tokens_asc">Menor gasto de tokens</option>
              <option value="model_asc">Modelo A-Z</option>
              <option value="project_asc">Proyecto A-Z</option>
              <option value="title_asc">Título A-Z</option>
            </select>
          </label>
          <label class="field">Estado
            <select id="threadArchiveFilter" class="control" aria-label="Filtrar threads por estado">
              <option value="all">Todos</option>
              <option value="active">Activos</option>
              <option value="archived">Archivados</option>
            </select>
          </label>
          <label class="field">Tokens mín.
            <input id="threadMinTokens" class="control" type="number" min="0" step="1000" placeholder="0" aria-label="Tokens mínimos">
          </label>
          <label class="field">Buscar
            <input id="threadSearch" class="control" type="search" placeholder="Título, ruta o id" aria-label="Buscar threads">
          </label>
          <label class="field">Mostrar
            <select id="threadLimit" class="control" aria-label="Número de threads a mostrar">
              <option value="30" selected>30</option>
              <option value="50">50</option>
              <option value="100">100</option>
              <option value="250">250</option>
            </select>
          </label>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr><th style="width: 30%">Título</th><th style="width: 26%">Proyecto</th><th>Modelo</th><th class="num">Tokens</th><th>Actualizado</th><th>Estado</th></tr></thead>
            <tbody id="threads"></tbody>
          </table>
        </div>
      </div>
    </section>
  </main>
  <div id="threadDrawer" class="drawer-backdrop" role="dialog" aria-modal="true" aria-labelledby="threadDialogTitle" hidden>
    <div class="thread-dialog">
      <div class="dialog-head">
        <div>
          <div class="label">Detalle del thread</div>
          <h2 id="threadDialogTitle" class="dialog-title">Thread</h2>
          <div id="threadDialogPills" class="row" style="justify-content:flex-start; flex-wrap:wrap;"></div>
        </div>
        <button id="closeThreadDrawer" class="dialog-close" type="button" aria-label="Cerrar detalle">X</button>
      </div>
      <div id="threadDialogBody"></div>
    </div>
  </div>
  <script>
    const nf = new Intl.NumberFormat('es-ES');
    const dateFmt = new Intl.DateTimeFormat('es-ES', { dateStyle: 'short', timeStyle: 'short' });
    const ALL = '__all__';
    const THEME_KEY = 'codex-dashboard-theme';
    const INVERT_LIMITS_KEY = 'codex-dashboard-invert-limits';
    let currentData = null;
    let invertLimits = false;
    let currentThreadDetailId = null;

    function fmt(value) { return nf.format(Math.round(value || 0)); }
    function esc(value) {
      return String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'}[char]));
    }
    function pct(value) { return Math.max(0, Math.min(100, Number(value || 0))); }
    function statusClass(value) { return value >= 90 ? 'danger' : value >= 70 ? 'warn' : ''; }
    function parseDate(value) {
      if (!value) return '-';
      const date = new Date(value);
      return Number.isNaN(date.getTime()) ? '-' : dateFmt.format(date);
    }
    function epochMs(value) {
      const ms = Date.parse(value || '');
      return Number.isNaN(ms) ? 0 : ms;
    }
    function cssVar(name) { return getComputedStyle(document.body).getPropertyValue(name).trim(); }
    function compareText(a, b) { return String(a || '').localeCompare(String(b || ''), 'es', { numeric: true, sensitivity: 'base' }); }
    function storageGet(key) { try { return localStorage.getItem(key); } catch { return null; } }
    function storageSet(key, value) { try { localStorage.setItem(key, value); } catch {} }
    function debounce(fn, delay = 120) {
      let timeout;
      return (...args) => { clearTimeout(timeout); timeout = setTimeout(() => fn(...args), delay); };
    }
    function projectFromPath(cwd) {
      const clean = String(cwd ?? '').replace('\\\\?\\', '').replace(/\\/g, '/').replace(/\/+$/, '');
      if (!clean) return '(sin proyecto)';
      const pieces = clean.split('/').filter(Boolean);
      return pieces[pieces.length - 1] || '(sin proyecto)';
    }
    function threadProject(row) { return row.project || projectFromPath(row.cwd); }
    function option(value, label) { return `<option value="${esc(value)}">${esc(label)}</option>`; }
    function setSelectOptions(select, rows, selected) {
      select.innerHTML = rows.map(([value, label]) => option(value, label)).join('');
      const hasSelected = Array.from(select.options).some(item => item.value === selected);
      select.value = hasSelected ? selected : rows[0]?.[0] || '';
    }
    function sortedUnique(values) {
      return Array.from(new Set(values.filter(Boolean))).sort((a, b) => compareText(a, b));
    }
    function limitBlock(name, item) {
      const used = pct(item?.used_percent);
      const displayPct = invertLimits ? Math.max(0, 100 - used) : used;
      const reset = item?.reset_at ? parseDate(new Date(item.reset_at * 1000).toISOString()) : '-';
      return `<div><div class="row"><strong>${esc(name)}</strong><span>${Math.round(displayPct)}%</span></div><div class="bar"><div class="fill ${statusClass(used)}" style="width:${displayPct}%"></div></div><div class="sub">${esc(item?.window_minutes || '-')} min · reset ${esc(reset)}</div></div>`;
    }
    function budgetBlock(name, used, budget) {
      if (!budget) return `<div><div class="row"><strong>${esc(name)}</strong><span>sin tope</span></div><div class="bar"><div class="fill" style="width:0%"></div></div></div>`;
      const usedPct = pct((used / budget) * 100);
      return `<div><div class="row"><strong>${esc(name)}</strong><span>${fmt(used)} / ${fmt(budget)} (${Math.round(usedPct)}%)</span></div><div class="bar"><div class="fill ${statusClass(usedPct)}" style="width:${usedPct}%"></div></div></div>`;
    }
    function applyTheme(theme) {
      const selected = theme === 'dark' ? 'dark' : 'light';
      document.body.dataset.theme = selected;
      storageSet(THEME_KEY, selected);
      const isDark = selected === 'dark';
      document.getElementById('themeToggle').setAttribute('aria-pressed', String(isDark));
      document.getElementById('themeIcon').textContent = isDark ? '☀' : '☾';
      document.getElementById('themeLabel').textContent = isDark ? 'Modo claro' : 'Modo oscuro';
      if (currentData) drawDaily(currentData.responses.by_day);
    }
    function initialTheme() {
      const saved = storageGet(THEME_KEY);
      if (saved === 'dark' || saved === 'light') return saved;
      return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }
    function drawDaily(rows) {
      const canvas = document.getElementById('dailyChart');
      const ctx = canvas.getContext('2d');
      const w = canvas.width, h = canvas.height;
      const pad = 36;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = cssVar('--chart-bg') || 'transparent';
      ctx.fillRect(0, 0, w, h);
      ctx.strokeStyle = cssVar('--line') || '#d9dee7';
      ctx.lineWidth = 1;
      for (let i = 0; i < 4; i++) {
        const y = pad + i * ((h - pad * 2) / 3);
        ctx.beginPath(); ctx.moveTo(pad, y); ctx.lineTo(w - pad, y); ctx.stroke();
      }
      ctx.fillStyle = cssVar('--muted') || '#53606f';
      ctx.font = '12px system-ui';
      if (!rows.length) {
        ctx.fillText('Sin datos en esta ventana', pad, h / 2);
        return;
      }
      const max = Math.max(1, ...rows.map(r => r.total));
      const step = (w - pad * 2) / Math.max(rows.length, 1);
      const barW = Math.max(3, Math.min(28, step - 6));
      rows.forEach((row, index) => {
        const x = pad + index * step + Math.max(0, (step - barW) / 2);
        const totalH = (row.total / max) * (h - pad * 2);
        const outH = (row.output / Math.max(row.total, 1)) * totalH;
        const y = h - pad - totalH;
        ctx.fillStyle = cssVar('--blue') || '#2563eb';
        ctx.beginPath();
        ctx.roundRect(x, y, barW, totalH, 5);
        ctx.fill();
        ctx.fillStyle = cssVar('--green') || '#0f9f6e';
        ctx.beginPath();
        ctx.roundRect(x, h - pad - outH, barW, outH, 5);
        ctx.fill();
      });
      ctx.fillStyle = cssVar('--muted') || '#53606f';
      ctx.fillText(fmt(max), pad, 17);
      ctx.fillText(rows[0].date.slice(5), pad, h - 10);
      ctx.fillText(rows[rows.length - 1].date.slice(5), w - pad - 38, h - 10);
    }
    function populateThreadFilters(data) {
      const projectSelect = document.getElementById('threadProjectFilter');
      const modelSelect = document.getElementById('threadModelFilter');
      const selectedProject = projectSelect.value || ALL;
      const selectedModel = modelSelect.value || ALL;
      const projects = sortedUnique(data.threads.map(threadProject));
      const models = sortedUnique(data.threads.map(row => row.model || 'desconocido'));
      setSelectOptions(projectSelect, [[ALL, 'Todos los proyectos'], ...projects.map(item => [item, item])], selectedProject);
      setSelectOptions(modelSelect, [[ALL, 'Todos los modelos'], ...models.map(item => [item, item])], selectedModel);
    }
    function filteredThreads() {
      if (!currentData) return [];
      const project = document.getElementById('threadProjectFilter').value || ALL;
      const model = document.getElementById('threadModelFilter').value || ALL;
      const state = document.getElementById('threadArchiveFilter').value || 'all';
      const minTokens = Number(document.getElementById('threadMinTokens').value || 0);
      const search = document.getElementById('threadSearch').value.trim().toLowerCase();
      const sort = document.getElementById('threadSort').value || 'updated_desc';
      return currentData.threads.filter(row => {
        const projectName = threadProject(row);
        if (project !== ALL && projectName !== project) return false;
        if (model !== ALL && (row.model || 'desconocido') !== model) return false;
        if (state === 'active' && row.archived) return false;
        if (state === 'archived' && !row.archived) return false;
        if (minTokens > 0 && Number(row.tokens_used || 0) < minTokens) return false;
        if (search) {
          const haystack = [row.title, row.cwd, row.model, row.id, projectName].join(' ').toLowerCase();
          if (!haystack.includes(search)) return false;
        }
        return true;
      }).sort((a, b) => {
        if (sort === 'tokens_desc') return (b.tokens_used || 0) - (a.tokens_used || 0) || epochMs(b.updated_at) - epochMs(a.updated_at);
        if (sort === 'tokens_asc') return (a.tokens_used || 0) - (b.tokens_used || 0) || epochMs(b.updated_at) - epochMs(a.updated_at);
        if (sort === 'model_asc') return compareText(a.model, b.model) || epochMs(b.updated_at) - epochMs(a.updated_at);
        if (sort === 'project_asc') return compareText(threadProject(a), threadProject(b)) || epochMs(b.updated_at) - epochMs(a.updated_at);
        if (sort === 'title_asc') return compareText(a.title, b.title) || epochMs(b.updated_at) - epochMs(a.updated_at);
        return epochMs(b.updated_at) - epochMs(a.updated_at);
      });
    }
    function detailItem(label, value) {
      const display = value === undefined || value === null || value === '' ? '-' : value;
      return `<div class="detail-item"><div class="label">${esc(label)}</div><div class="detail-value">${esc(display)}</div></div>`;
    }
    function openThreadDetail(threadId) {
      if (!currentData) return;
      const row = currentData.threads.find(item => item.id === threadId);
      if (!row) return;
      currentThreadDetailId = threadId;
      const projectName = threadProject(row);
      document.getElementById('threadDialogTitle').textContent = row.title || 'Thread';
      document.getElementById('threadDialogPills').innerHTML = [
        `<span class="pill project">${esc(projectName)}</span>`,
        `<span class="pill model">${esc(row.model || 'desconocido')}</span>`,
        `<span class="pill ${row.archived ? 'archived' : 'active'}">${row.archived ? 'Archivado' : 'Activo'}</span>`,
      ].join('');
      document.getElementById('threadDialogBody').innerHTML = `
        <div class="detail-grid">
          ${detailItem('Tokens usados', fmt(row.tokens_used))}
          ${detailItem('Reasoning', row.reasoning_effort || '-')}
          ${detailItem('Creado', parseDate(row.created_at))}
          ${detailItem('Actualizado', parseDate(row.updated_at))}
          ${detailItem('Origen', row.source || '-')}
          ${detailItem('CLI', row.cli_version || '-')}
          ${detailItem('Rama Git', row.git_branch || '-')}
          ${detailItem('SHA Git', row.git_sha || '-')}
          ${detailItem('Agente', [row.agent_nickname, row.agent_role].filter(Boolean).join(' / ') || '-')}
          ${detailItem('Thread ID', row.id || '-')}
          ${detailItem('Proyecto', projectName)}
          ${detailItem('Ruta', row.cwd || '-')}
          ${detailItem('Remoto Git', row.git_origin_url || '-')}
        </div>
        <div class="detail-item" style="margin-top:12px;">
          <div class="label">Primer mensaje</div>
          <div class="detail-value detail-message">${esc(row.first_user_message || row.title || '-')}</div>
        </div>
      `;
      const drawer = document.getElementById('threadDrawer');
      drawer.hidden = false;
      drawer.classList.add('open');
      document.getElementById('closeThreadDrawer').focus();
    }
    function closeThreadDetail() {
      const drawer = document.getElementById('threadDrawer');
      drawer.classList.remove('open');
      drawer.hidden = true;
      const previousId = currentThreadDetailId;
      currentThreadDetailId = null;
      if (previousId) {
        const row = document.querySelector(`[data-thread-id="${CSS.escape(previousId)}"]`);
        if (row) row.focus();
      }
    }
    function renderThreads() {
      if (!currentData) return;
      const rows = filteredThreads();
      const limit = Number(document.getElementById('threadLimit').value || 30);
      const visible = rows.slice(0, limit);
      document.getElementById('threadMeta').textContent = `${fmt(rows.length)} coincidencias · ${fmt(currentData.threads.length)} threads cargados`;
      document.getElementById('threads').innerHTML = visible.map((row, index) => {
        const projectName = threadProject(row);
        const delay = Math.min(index, 12) * 18;
        return `<tr class="thread-row" tabindex="0" data-thread-id="${esc(row.id)}" style="animation-delay:${delay}ms" title="Ver detalle del thread">
          <td><div class="truncate" title="${esc(row.title)}"><strong>${esc(row.title)}</strong></div><div class="path truncate" title="${esc(row.id)}">${esc(row.id || '')}</div></td>
          <td><span class="pill project" title="${esc(projectName)}">${esc(projectName)}</span><div class="path truncate" title="${esc(row.cwd)}">${esc(row.cwd || '-')}</div></td>
          <td><span class="pill model" title="${esc(row.model)}">${esc(row.model)}</span></td>
          <td class="num"><strong>${fmt(row.tokens_used)}</strong></td>
          <td>${parseDate(row.updated_at)}</td>
          <td><span class="pill ${row.archived ? 'archived' : 'active'}">${row.archived ? 'Archivado' : 'Activo'}</span></td>
        </tr>`;
      }).join('') || '<tr><td colspan="6" class="empty">No hay threads para esos filtros.</td></tr>';
    }
    function render(data) {
      currentData = data;
      document.getElementById('totalTokens').textContent = fmt(data.response_totals.total);
      document.getElementById('totalTokensSub').textContent = `${fmt(data.response_totals.input)} input · ${fmt(data.response_totals.output)} output`;
      document.getElementById('calls').textContent = fmt(data.response_totals.calls);
      document.getElementById('threadTokens').textContent = fmt(data.thread_totals.tokens);
      document.getElementById('threadCount').textContent = `${fmt(data.thread_totals.count)} threads en state_5.sqlite`;
      document.getElementById('cached').textContent = fmt(data.response_totals.cached);
      document.getElementById('lastUpdated').textContent = data.generated_at ? `Actualizado ${parseDate(data.generated_at)}` : 'actualizado';

      const limits = data.rate_limits?.rate_limits;
      document.getElementById('plan').textContent = data.rate_limits ? `Plan: ${data.rate_limits.plan_type || 'desconocido'} · fuente: ${data.rate_limits.source || 'local'}` : 'No hay evento de límites en los datos locales.';
      document.getElementById('limitSeen').textContent = data.rate_limits?.seen_at ? parseDate(data.rate_limits.seen_at) : 'sin datos';
      document.getElementById('limits').innerHTML = limits ? [
        limitBlock('Ventana primaria', limits.primary),
        limitBlock('Ventana secundaria', limits.secondary)
      ].join('') : '<p class="warnText">Abre Codex o ejecuta una conversación para que aparezca el último evento de límites.</p>';

      document.getElementById('dailyBudget').value = data.config?.token_budget?.daily || 0;
      document.getElementById('monthlyBudget').value = data.config?.token_budget?.monthly || 0;
      const today = new Date().toISOString().slice(0, 10);
      const todayUsed = (data.responses.by_day.find(row => row.date === today) || {}).total || 0;
      document.getElementById('budgetBars').innerHTML = [
        budgetBlock('Hoy', todayUsed, Number(document.getElementById('dailyBudget').value)),
        budgetBlock('Ventana actual', data.response_totals.total, Number(document.getElementById('monthlyBudget').value))
      ].join('');

      document.getElementById('models').innerHTML = data.model_usage.map((row, index) => `
        <tr style="animation-delay:${Math.min(index, 10) * 18}ms">
          <td><span class="pill model">${esc(row.model)}</span></td>
          <td class="num">${fmt(row.threads)}</td>
          <td class="num">${fmt(row.thread_tokens)}</td>
          <td class="num">${fmt(row.window_calls)}</td>
          <td class="num"><strong>${fmt(row.window_tokens)}</strong></td>
          <td>${fmt(row.window_input)} / ${fmt(row.window_output)}</td>
        </tr>
      `).join('') || '<tr><td colspan="6" class="empty">Sin modelos registrados.</td></tr>';

      populateThreadFilters(data);
      renderThreads();
      drawDaily(data.responses.by_day);
    }
    async function load(options = {}) {
      const silent = Boolean(options.silent);
      const refreshButton = document.getElementById('refresh');
      if (!silent) refreshButton.classList.add('loading');
      try {
        const days = document.getElementById('days').value;
        const res = await fetch(`/api/summary?days=${encodeURIComponent(days)}&t=${Date.now()}`, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text());
        render(await res.json());
      } finally {
        refreshButton.classList.remove('loading');
      }
    }

    applyTheme(initialTheme());
    invertLimits = storageGet(INVERT_LIMITS_KEY) === 'true';
    document.getElementById('toggleLimitsBtn').textContent = invertLimits ? 'Mostrar Consumido' : 'Mostrar Restante';

    document.getElementById('toggleLimitsBtn').addEventListener('click', () => {
      invertLimits = !invertLimits;
      storageSet(INVERT_LIMITS_KEY, String(invertLimits));
      document.getElementById('toggleLimitsBtn').textContent = invertLimits ? 'Mostrar Consumido' : 'Mostrar Restante';
      if (currentData) render(currentData);
    });

    document.getElementById('themeToggle').addEventListener('click', () => {
      applyTheme(document.body.dataset.theme === 'dark' ? 'light' : 'dark');
    });
    document.getElementById('refresh').addEventListener('click', () => load().catch(showError));
    document.getElementById('days').addEventListener('change', () => load().catch(showError));
    ['threadProjectFilter', 'threadModelFilter', 'threadSort', 'threadArchiveFilter', 'threadLimit'].forEach(id => {
      document.getElementById(id).addEventListener('change', renderThreads);
    });
    ['threadSearch', 'threadMinTokens'].forEach(id => {
      document.getElementById(id).addEventListener('input', debounce(renderThreads));
    });
    document.getElementById('threads').addEventListener('click', event => {
      const row = event.target.closest('.thread-row');
      if (row?.dataset.threadId) openThreadDetail(row.dataset.threadId);
    });
    document.getElementById('threads').addEventListener('keydown', event => {
      if (event.key !== 'Enter' && event.key !== ' ') return;
      const row = event.target.closest('.thread-row');
      if (!row?.dataset.threadId) return;
      event.preventDefault();
      openThreadDetail(row.dataset.threadId);
    });
    document.getElementById('closeThreadDrawer').addEventListener('click', closeThreadDetail);
    document.getElementById('threadDrawer').addEventListener('click', event => {
      if (event.target.id === 'threadDrawer') closeThreadDetail();
    });
    document.addEventListener('keydown', event => {
      if (event.key === 'Escape' && !document.getElementById('threadDrawer').hidden) closeThreadDetail();
    });
    document.getElementById('clearThreadFilters').addEventListener('click', () => {
      document.getElementById('threadProjectFilter').value = ALL;
      document.getElementById('threadModelFilter').value = ALL;
      document.getElementById('threadSort').value = 'updated_desc';
      document.getElementById('threadArchiveFilter').value = 'all';
      document.getElementById('threadMinTokens').value = '';
      document.getElementById('threadSearch').value = '';
      document.getElementById('threadLimit').value = '30';
      renderThreads();
    });
    document.getElementById('saveSettings').addEventListener('click', async () => {
      const payload = { token_budget: {
        daily: Number(document.getElementById('dailyBudget').value || 0),
        monthly: Number(document.getElementById('monthlyBudget').value || 0)
      }};
      const res = await fetch('/api/settings', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload) });
      if (!res.ok) alert(await res.text());
      await load();
    });
    function showError(error) {
      const message = esc(error?.message || String(error));
      document.body.insertAdjacentHTML('afterbegin', `<div role="alert" class="error" style="padding:16px">${message}</div>`);
    }
    load().catch(showError);
  </script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def send_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json; charset=utf-8")
        self.send_header("cache-control", "no-store")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            body = HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("content-type", "text/html; charset=utf-8")
            self.send_header("cache-control", "no-store")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/api/summary":
            params = parse_qs(parsed.query)
            try:
                days = int(params.get("days", ["30"])[0])
            except ValueError:
                days = 30
            self.send_json(build_summary(days))
            return
        self.send_error(404, "Not found")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/api/settings":
            self.send_error(404, "Not found")
            return
        length = int(self.headers.get("content-length", "0"))
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except json.JSONDecodeError:
            self.send_error(400, "Invalid JSON")
            return
        current = read_json(CONFIG_FILE, {"token_budget": {"monthly": 0, "daily": 0}, "notes": ""})
        budget = payload.get("token_budget", {})
        current["token_budget"] = {
            "daily": max(0, int(budget.get("daily", 0) or 0)),
            "monthly": max(0, int(budget.get("monthly", 0) or 0)),
        }
        write_json(CONFIG_FILE, current)
        self.send_json({"ok": True, "config": current})

    def log_message(self, fmt: str, *args: object) -> None:
        print("%s - %s" % (self.address_string(), fmt % args))


def main() -> None:
    port = int(os.environ.get("PORT", "8765"))
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"Dashboard listo en http://127.0.0.1:{port}")
    print(f"Leyendo datos de {CODEX_DIR}")
    server.serve_forever()


if __name__ == "__main__":
    main()
