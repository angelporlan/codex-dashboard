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
                   created_at, updated_at, archived, source
            from threads
            order by updated_at desc
            """
        ).fetchall()

    threads = []
    for row in rows:
        threads.append(
            {
                "id": row["id"],
                "title": row["title"] or "(sin titulo)",
                "cwd": (row["cwd"] or "").replace("\\\\?\\", ""),
                "model": row["model"] or "desconocido",
                "reasoning_effort": row["reasoning_effort"] or "",
                "tokens_used": row["tokens_used"] or 0,
                "created_at": epoch_to_iso(row["created_at"]),
                "updated_at": epoch_to_iso(row["updated_at"]),
                "archived": bool(row["archived"]),
                "source": row["source"],
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
        "threads": threads["threads"][:100],
    }


HTML = r"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Codex Usage Dashboard</title>
  <style>
    :root {
      --bg: #f7f8fa;
      --panel: #ffffff;
      --ink: #111827;
      --muted: #53606f;
      --line: #d9dee7;
      --blue: #2563eb;
      --green: #0f9f6e;
      --amber: #d97706;
      --red: #dc2626;
      --violet: #7c3aed;
      --shadow: 0 14px 32px rgba(17, 24, 39, .08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      background: var(--bg);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif;
    }
    button, input, select { font: inherit; }
    .shell { max-width: 1320px; margin: 0 auto; padding: 24px; }
    header {
      display: flex; justify-content: space-between; gap: 16px; align-items: flex-start;
      padding: 10px 0 22px;
    }
    h1 { font-size: clamp(28px, 4vw, 44px); line-height: 1; margin: 0 0 10px; letter-spacing: 0; }
    p { margin: 0; color: var(--muted); }
    .controls { display: flex; gap: 10px; flex-wrap: wrap; justify-content: flex-end; }
    .control, button {
      min-height: 40px; border: 1px solid var(--line); background: var(--panel);
      border-radius: 8px; padding: 8px 12px; color: var(--ink);
    }
    button { cursor: pointer; transition: border-color .18s, background .18s; }
    button:hover { border-color: var(--blue); }
    .grid { display: grid; grid-template-columns: repeat(12, 1fr); gap: 14px; }
    .panel {
      background: var(--panel); border: 1px solid var(--line); border-radius: 8px;
      box-shadow: var(--shadow); padding: 16px; min-width: 0;
    }
    .metric-card { position: relative; padding-right: 48px; }
    .info {
      position: absolute; top: 14px; right: 14px; width: 28px; height: 28px;
      display: inline-grid; place-items: center; border: 1px solid var(--line);
      border-radius: 999px; color: var(--muted); background: #f9fafb; cursor: help;
      transition: color .18s, border-color .18s, background .18s;
    }
    .info:hover, .info:focus { color: var(--blue); border-color: var(--blue); background: #eff6ff; outline: none; }
    .info svg { width: 16px; height: 16px; stroke-width: 2.2; }
    .tip {
      position: absolute; z-index: 5; top: 42px; right: 0; width: min(260px, 72vw);
      padding: 10px 12px; border: 1px solid #c7d2fe; border-radius: 8px;
      background: #ffffff; color: var(--ink); box-shadow: 0 16px 38px rgba(17, 24, 39, .16);
      font-size: 13px; line-height: 1.35; font-weight: 500; opacity: 0; visibility: hidden;
      transform: translateY(-4px); transition: opacity .16s, transform .16s, visibility .16s;
    }
    .info:hover .tip, .info:focus .tip { opacity: 1; visibility: visible; transform: translateY(0); }
    .span-3 { grid-column: span 3; }
    .span-4 { grid-column: span 4; }
    .span-5 { grid-column: span 5; }
    .span-7 { grid-column: span 7; }
    .span-8 { grid-column: span 8; }
    .span-12 { grid-column: span 12; }
    .label { color: var(--muted); font-size: 13px; font-weight: 700; text-transform: uppercase; }
    .metric { font-size: 30px; font-weight: 800; margin-top: 8px; }
    .sub { color: var(--muted); font-size: 13px; margin-top: 6px; }
    .bar { height: 10px; border-radius: 999px; background: #edf0f4; overflow: hidden; margin-top: 12px; }
    .fill { height: 100%; width: 0%; background: var(--blue); transition: width .3s; }
    .fill.warn { background: var(--amber); }
    .fill.danger { background: var(--red); }
    .row { display: flex; justify-content: space-between; gap: 12px; align-items: center; }
    .stack { display: grid; gap: 12px; }
    canvas { width: 100%; height: 260px; display: block; }
    table { width: 100%; border-collapse: collapse; table-layout: fixed; }
    th, td { padding: 11px 10px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }
    th { color: var(--muted); font-size: 12px; text-transform: uppercase; }
    td { font-size: 14px; }
    .truncate { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .pill { display: inline-flex; border: 1px solid var(--line); border-radius: 999px; padding: 3px 8px; font-size: 12px; color: var(--muted); }
    .settings { display: flex; gap: 10px; flex-wrap: wrap; align-items: end; }
    .settings label { display: grid; gap: 5px; color: var(--muted); font-size: 13px; font-weight: 700; }
    .settings input { width: 160px; }
    .ok { color: var(--green); }
    .warnText { color: var(--amber); }
    .error { color: var(--red); }
    @media (max-width: 960px) {
      header { display: grid; }
      .controls { justify-content: start; }
      .span-3, .span-4, .span-5, .span-7, .span-8 { grid-column: span 12; }
      .metric { font-size: 25px; }
      .shell { padding: 16px; }
    }
  </style>
</head>
<body>
  <main class="shell">
    <header>
      <div>
        <h1>Codex Usage</h1>
        <p>Tokens, sesiones y límites leídos desde tu instalación local de Codex.</p>
      </div>
      <div class="controls">
        <select id="days" class="control" aria-label="Ventana de dias">
          <option value="7">7 dias</option>
          <option value="30" selected>30 dias</option>
          <option value="90">90 dias</option>
          <option value="365">365 dias</option>
        </select>
        <button id="refresh" type="button" title="Actualizar">Actualizar</button>
      </div>
    </header>

    <section class="grid" aria-live="polite">
      <div class="panel span-3 metric-card">
        <button class="info" type="button" aria-label="Que significa tokens en ventana">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true"><circle cx="12" cy="12" r="10"></circle><path d="M12 16v-4"></path><path d="M12 8h.01"></path></svg>
          <span class="tip">Suma de tokens de entrada y salida detectados en los eventos response.completed dentro de la ventana seleccionada.</span>
        </button>
        <div class="label">Tokens en ventana</div><div class="metric" id="totalTokens">-</div><div class="sub" id="totalTokensSub">input + output</div>
      </div>
      <div class="panel span-3 metric-card">
        <button class="info" type="button" aria-label="Que significa llamadas medidas">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true"><circle cx="12" cy="12" r="10"></circle><path d="M12 16v-4"></path><path d="M12 8h.01"></path></svg>
          <span class="tip">Numero de respuestas de Codex con metricas de tokens encontradas en los logs locales para esta ventana.</span>
        </button>
        <div class="label">Llamadas medidas</div><div class="metric" id="calls">-</div><div class="sub">Eventos response.completed</div>
      </div>
      <div class="panel span-3 metric-card">
        <button class="info" type="button" aria-label="Que significa tokens por threads">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true"><circle cx="12" cy="12" r="10"></circle><path d="M12 16v-4"></path><path d="M12 8h.01"></path></svg>
          <span class="tip">Total acumulado que Codex guarda por conversacion en state_5.sqlite. Sirve como historico local amplio.</span>
        </button>
        <div class="label">Tokens por threads</div><div class="metric" id="threadTokens">-</div><div class="sub" id="threadCount">Estado local acumulado</div>
      </div>
      <div class="panel span-3 metric-card">
        <button class="info" type="button" aria-label="Que significa cache">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true"><circle cx="12" cy="12" r="10"></circle><path d="M12 16v-4"></path><path d="M12 8h.01"></path></svg>
          <span class="tip">Tokens marcados como cacheados por Codex. Pueden reducir coste o carga real, pero no equivalen siempre a gasto facturable.</span>
        </button>
        <div class="label">Cache</div><div class="metric" id="cached">-</div><div class="sub">Tokens cacheados registrados</div>
      </div>

      <div class="panel span-5">
        <div class="row"><div><div class="label">Limites de Codex</div><p id="plan">Buscando evento local reciente</p></div><span class="pill" id="limitSeen">-</span></div>
        <div class="stack" id="limits"></div>
      </div>

      <div class="panel span-7">
        <div class="row"><div><div class="label">Tendencia diaria</div><p>Uso input/output dentro de la ventana seleccionada.</p></div></div>
        <canvas id="dailyChart" width="900" height="260" aria-label="Grafico diario de tokens"></canvas>
      </div>

      <div class="panel span-4">
        <div class="label">Presupuesto local</div>
        <p>Opcional: define tus propios topes de tokens para compararlos con el consumo.</p>
        <div class="settings" style="margin-top: 12px;">
          <label>Diario <input id="dailyBudget" class="control" type="number" min="0" step="1000"></label>
          <label>Mensual <input id="monthlyBudget" class="control" type="number" min="0" step="1000"></label>
          <button id="saveSettings" type="button">Guardar</button>
        </div>
        <div id="budgetBars" class="stack" style="margin-top: 14px;"></div>
      </div>

      <div class="panel span-8">
        <div class="label">Modelos</div>
        <p>Historico local por threads y detalle medido en la ventana seleccionada.</p>
        <table>
          <thead><tr><th>Modelo</th><th>Threads</th><th>Tokens historicos</th><th>Llamadas ventana</th><th>Tokens ventana</th><th>Input / Output</th></tr></thead>
          <tbody id="models"></tbody>
        </table>
      </div>

      <div class="panel span-12">
        <div class="label">Threads recientes</div>
        <table>
          <thead><tr><th style="width: 36%">Titulo</th><th>Proyecto</th><th>Modelo</th><th>Tokens</th><th>Actualizado</th></tr></thead>
          <tbody id="threads"></tbody>
        </table>
      </div>
    </section>
  </main>
  <script>
    const nf = new Intl.NumberFormat('es-ES');
    const dateFmt = new Intl.DateTimeFormat('es-ES', { dateStyle: 'short', timeStyle: 'short' });
    let currentData = null;

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
    function limitBlock(name, item) {
      const used = pct(item?.used_percent);
      const reset = item?.reset_at ? parseDate(new Date(item.reset_at * 1000).toISOString()) : '-';
      return `<div><div class="row"><strong>${name}</strong><span>${used}%</span></div><div class="bar"><div class="fill ${statusClass(used)}" style="width:${used}%"></div></div><div class="sub">${item?.window_minutes || '-'} min - reset ${reset}</div></div>`;
    }
    function budgetBlock(name, used, budget) {
      if (!budget) return `<div><div class="row"><strong>${name}</strong><span>sin tope</span></div><div class="bar"><div class="fill" style="width:0%"></div></div></div>`;
      const usedPct = pct((used / budget) * 100);
      return `<div><div class="row"><strong>${name}</strong><span>${fmt(used)} / ${fmt(budget)} (${Math.round(usedPct)}%)</span></div><div class="bar"><div class="fill ${statusClass(usedPct)}" style="width:${usedPct}%"></div></div></div>`;
    }
    function drawDaily(rows) {
      const canvas = document.getElementById('dailyChart');
      const ctx = canvas.getContext('2d');
      const w = canvas.width, h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = '#ffffff';
      ctx.fillRect(0, 0, w, h);
      const pad = 34;
      const max = Math.max(1, ...rows.map(r => r.total));
      const barW = Math.max(4, (w - pad * 2) / Math.max(rows.length, 1) - 6);
      ctx.strokeStyle = '#d9dee7';
      ctx.lineWidth = 1;
      for (let i = 0; i < 4; i++) {
        const y = pad + i * ((h - pad * 2) / 3);
        ctx.beginPath(); ctx.moveTo(pad, y); ctx.lineTo(w - pad, y); ctx.stroke();
      }
      rows.forEach((row, index) => {
        const x = pad + index * ((w - pad * 2) / Math.max(rows.length, 1));
        const totalH = (row.total / max) * (h - pad * 2);
        const outH = (row.output / Math.max(row.total, 1)) * totalH;
        ctx.fillStyle = '#2563eb';
        ctx.fillRect(x, h - pad - totalH, barW, totalH);
        ctx.fillStyle = '#0f9f6e';
        ctx.fillRect(x, h - pad - outH, barW, outH);
      });
      ctx.fillStyle = '#53606f';
      ctx.font = '12px system-ui';
      ctx.fillText(fmt(max), pad, 14);
      if (rows.length) {
        ctx.fillText(rows[0].date.slice(5), pad, h - 8);
        ctx.fillText(rows[rows.length - 1].date.slice(5), w - pad - 38, h - 8);
      }
    }
    function render(data) {
      currentData = data;
      document.getElementById('totalTokens').textContent = fmt(data.response_totals.total);
      document.getElementById('totalTokensSub').textContent = `${fmt(data.response_totals.input)} input - ${fmt(data.response_totals.output)} output`;
      document.getElementById('calls').textContent = fmt(data.response_totals.calls);
      document.getElementById('threadTokens').textContent = fmt(data.thread_totals.tokens);
      document.getElementById('threadCount').textContent = `${fmt(data.thread_totals.count)} threads en state_5.sqlite`;
      document.getElementById('cached').textContent = fmt(data.response_totals.cached);

      const limits = data.rate_limits?.rate_limits;
      document.getElementById('plan').textContent = data.rate_limits ? `Plan: ${data.rate_limits.plan_type || 'desconocido'} - fuente: ${data.rate_limits.source || 'local'}` : 'No hay evento de limites en los datos locales.';
      document.getElementById('limitSeen').textContent = data.rate_limits?.seen_at ? parseDate(data.rate_limits.seen_at) : 'sin datos';
      document.getElementById('limits').innerHTML = limits ? [
        limitBlock('Ventana primaria', limits.primary),
        limitBlock('Ventana secundaria', limits.secondary)
      ].join('') : '<p class="warnText">Abre Codex o ejecuta una conversacion para que aparezca el ultimo evento de limites.</p>';

      document.getElementById('dailyBudget').value = data.config?.token_budget?.daily || 0;
      document.getElementById('monthlyBudget').value = data.config?.token_budget?.monthly || 0;
      const today = new Date().toISOString().slice(0, 10);
      const todayUsed = (data.responses.by_day.find(row => row.date === today) || {}).total || 0;
      document.getElementById('budgetBars').innerHTML = [
        budgetBlock('Hoy', todayUsed, Number(document.getElementById('dailyBudget').value)),
        budgetBlock('Ventana actual', data.response_totals.total, Number(document.getElementById('monthlyBudget').value))
      ].join('');

      document.getElementById('models').innerHTML = data.model_usage.map(row => `
        <tr>
          <td><span class="pill">${esc(row.model)}</span></td>
          <td>${fmt(row.threads)}</td>
          <td>${fmt(row.thread_tokens)}</td>
          <td>${fmt(row.window_calls)}</td>
          <td>${fmt(row.window_tokens)}</td>
          <td>${fmt(row.window_input)} / ${fmt(row.window_output)}</td>
        </tr>
      `).join('') || '<tr><td colspan="6">Sin modelos registrados.</td></tr>';

      document.getElementById('threads').innerHTML = data.threads.slice(0, 30).map(row => `
        <tr><td class="truncate" title="${esc(row.title)}">${esc(row.title)}</td><td class="truncate" title="${esc(row.cwd)}">${esc(row.cwd)}</td><td>${esc(row.model)}</td><td>${fmt(row.tokens_used)}</td><td>${parseDate(row.updated_at)}</td></tr>
      `).join('');
      drawDaily(data.responses.by_day);
    }
    async function load() {
      const days = document.getElementById('days').value;
      const res = await fetch(`/api/summary?days=${encodeURIComponent(days)}&t=${Date.now()}`, { cache: 'no-store' });
      if (!res.ok) throw new Error(await res.text());
      render(await res.json());
    }
    document.getElementById('refresh').addEventListener('click', load);
    document.getElementById('days').addEventListener('change', load);
    document.getElementById('saveSettings').addEventListener('click', async () => {
      const payload = { token_budget: {
        daily: Number(document.getElementById('dailyBudget').value || 0),
        monthly: Number(document.getElementById('monthlyBudget').value || 0)
      }};
      const res = await fetch('/api/settings', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload) });
      if (!res.ok) alert(await res.text());
      await load();
    });
    load().catch(error => {
      document.body.insertAdjacentHTML('afterbegin', `<div role="alert" class="error" style="padding:16px">${error.message}</div>`);
    });
    setInterval(() => load().catch(() => {}), 15000);
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
