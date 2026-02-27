#!/usr/bin/env python3
"""
Read CMAPI tracing JSONL logs (JsonFormatter) from one or more nodes and render a
PlantUML sequence diagram showing inbound (SERVER) and outbound (CLIENT) requests
between nodes.

Inputs
  - One or more JSONL log files
  - Each input log file is assumed to be from a single node.

Output
  - PlantUML written to the specified output file.

Parsing and correlation
  - Detects span_begin/span_end events from JSONL (msg in {"span_begin","span_end"}).
  - Per file, reads the "my_addresses" record and takes the first non-loopback IP (preferring IPv4)
    as the node's local IP; all spans from that file carry this local IP.
  - Merge spans across files by (trace_id, span_id). Duration and status_code are added when present.

Normalization and aliasing
  - Localhost remap: any 127.0.0.1/::1 seen in that file is replaced with the file's local IP.
  - Remote host normalization: hostnames with embedded IPs (dashed or dotted) are converted to dotted IPs.
  - Aliasing: if an IP is provided via --alias IP=NAME, the diagram uses NAME for that participant.

Diagram semantics
  - SERVER spans: "<remote> --> METHOD /path" render as remote -> local (local is the file's node).
  - CLIENT spans: "HTTP METHOD https://host/path" render as local -> remote.
  - CLIENT spans prefer request_duration_ms; otherwise duration_ms is shown if available.
  - SERVER spans include [status_code] when available.

CLI options
  - --filter-trace TRACE_ID   Only include the specified trace_id.
  - --alias IP=NAME           Map an IP to a friendly name (may be specified multiple times).
  - --list-ips                Print the set of encountered IPs and exit.

Usage examples
  python3 cmapi/scripts/trace_to_plantuml.py json_trace_mcs1 json_trace_mcs2 json_trace_mcs3 --output diagram.puml
  python3 cmapi/scripts/trace_to_plantuml.py json_trace_mcs1 json_trace_mcs2 json_trace_mcs3 --output diagram.puml
      --filter-trace 1f3b2cb2... --alias 172.31.41.127=mcs2 --alias 172.31.45.34=mcs3
"""
from __future__ import annotations

import argparse
import colorsys
import dataclasses
import datetime as dt
import hashlib
import ipaddress
import json
import logging
import os
import re
import sys
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

JSON_TS_FORMATS = [
    "%Y-%m-%d %H:%M:%S,%f",  # default logging formatter
    "%Y-%m-%d %H:%M:%S",     # without millis
]

# Minimum processing time to show activation lifeline (in milliseconds)
ACTIVATION_MIN_MS = 1000

# dashed IP fragment pattern; will convert '172-31-45-34' -> '172.31.45.34'
DASHED_IP_RE = re.compile(r"(?P<dashed>\b\d{1,3}(?:-\d{1,3}){3}\b)")

logger = logging.getLogger("trace_viz")

def _normalize_cmapi_path(path: str) -> str:
    """Remove common CMAPI prefixes like '/cmapi/0.4.0' and '/cmapi/'.
    """
    if not path:
        return "/"
    # normalize double slashes
    while '//' in path:
        path = path.replace('//', '/')
    prefixes = [
        '/cmapi/0.4.0',
        '/cmapi/',
    ]
    for pref in prefixes:
        if path.startswith(pref):
            path = path[len(pref):]
            break
    if not path.startswith('/'):
        path = '/' + path
    return path if path != '//' else '/'

def _hex_color_for_trace(trace_id: str, depth: int) -> str:
    """Derive a stable color from trace_id and vary by depth.

    We map trace_id hash to a base hue and then shift lightness slightly per depth.
    Returns a hex color string like '#a1b2c3'.
    """
    h = hashlib.sha1(trace_id.encode('utf-8')).digest()
    # base hue from first byte, saturation fixed, lightness varies by depth
    base_hue = h[0] / 255.0  # 0..1
    sat = 0.55
    base_lightness = 0.55
    # depth shift: +/- 0.06 per level alternating
    shift = ((depth % 6) - 3) * 0.02  # -0.06..+0.06
    lightness = max(0.35, min(0.75, base_lightness + shift))
    # convert HSL to RGB
    r, g, b = colorsys.hls_to_rgb(base_hue, lightness, sat)
    return f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"


def _colorized_arrow(arrow: str, color_hex: str) -> str:
    """Insert a PlantUML color into an arrow token.

    Examples:
      '->'   -> '-[#hex]->'
      '-->'  -> '-[#hex]-->'
      '->>'  -> '-[#hex]->>'
      '-->>' -> '-[#hex]-->>'
    """
    if not color_hex:
        return arrow
    if arrow.startswith('--'):
        return f"-[{color_hex}]{arrow}"
    return f"-[{color_hex}]{arrow}"


def _fmt_secs(ms: float) -> str:
    """Format milliseconds as seconds with 2 decimal places."""
    return f"{(ms or 0.0)/1000.0:.2f}s"

@dataclasses.dataclass
class Span:
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    kind: str  # SERVER or CLIENT
    name: str
    start_ts: dt.datetime
    duration_ms: Optional[float] = None
    status_code: Optional[int] = None
    request_duration_ms: Optional[float] = None
    local_label: str = "local"
    local_ip: Optional[str] = None
    local_file: Optional[str] = None  # full path to source log file
    # Structured attributes
    http_method: Optional[str] = None
    http_path: Optional[str] = None
    http_url: Optional[str] = None
    client_ip: Optional[str] = None
    request_is_sync: Optional[bool] = None


@dataclasses.dataclass
class Message:
    ts: dt.datetime
    src: str
    dst: str
    label: str
    arrow: str  # '->', '->>', '-->', '-->>'


@dataclasses.dataclass
class Directive:
    ts: dt.datetime
    text: str  # e.g., 'activate mcs1' or 'deactivate mcs1'


def parse_json_ts(ts_str: str) -> Optional[dt.datetime]:
    for fmt in JSON_TS_FORMATS:
        try:
            return dt.datetime.strptime(ts_str, fmt)
        except Exception:
            continue
    return None


def parse_json_span_event(obj: Dict[str, object]) -> Optional[Tuple[str, Dict[str, object]]]:
    """Return (msg, data) if this JSON object is a span_begin/span_end with usable fields."""
    msg = str(obj.get("msg", ""))
    if msg not in {"span_begin", "span_end"}:
        return None
    return msg, obj


def local_label_from_path(path: str) -> str:
    base = os.path.basename(path)
    return base


def collect_known_ips_from_span(sp: "Span", known_ips: set[str]) -> None:
    """Populate known IPs using structured span attributes only."""
    try:
        if sp.kind == "SERVER" and sp.client_ip and is_ip(sp.client_ip):
            known_ips.add(sp.client_ip)
        elif sp.kind == "CLIENT" and sp.http_url:
            try:
                host = urlparse(sp.http_url).hostname
            except Exception:
                host = None
            if host and is_ip(host):
                known_ips.add(host)
    except Exception:
        pass


def normalize_participant(name: str) -> str:
    # PlantUML participant names cannot contain spaces or some punctuation unless quoted.
    # We'll use an alias form: participant "label" as alias, where alias is alnum/underscore.
    # This function returns a safe alias; the label is kept original elsewhere.
    alias = re.sub(r"[^A-Za-z0-9_]", "_", name)
    # Avoid alias starting with a digit
    if alias and alias[0].isdigit():
        alias = f"n_{alias}"
    return alias or "unknown"


# Legacy name-parsing helpers removed; rely on structured attributes.


def is_ip(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
        return True
    except Exception:
        return False


def parse_jsonl_logs(log_paths: List[str]) -> Tuple[List[Span], List[str]]:
    spans_by_key: Dict[Tuple[str, str], Span] = {}
    known_ips: set[str] = set()
    for path in log_paths:
        local_label = local_label_from_path(path)
        # Per-file local IP discovered from a special 'my_addresses' record
        file_local_ip: Optional[str] = None
        if not os.path.exists(path):
            logger.error("Input log file does not exist: %s", path)
            raise FileNotFoundError(path)
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception as exc:
                    logger.error("Failed to parse JSON line in %s: %s", path, line[:256])
                    raise ValueError(f"JSON parse error in {path}: {exc}")
                # Detect and store local IPs for this file
                if str(obj.get("msg", "")) == "my_addresses":
                    addrs = obj.get("my_addresses")
                    if isinstance(addrs, list):
                        # Pick first non-loopback IPv4 if available, else first non-loopback
                        candidates = [a for a in addrs if isinstance(a, str) and a not in {"127.0.0.1", "::1"}]
                        # Prefer IPv4
                        ipv4 = [a for a in candidates if re.match(r"^\d{1,3}(?:\.\d{1,3}){3}$", a)]
                        chosen = ipv4[0] if ipv4 else (candidates[0] if candidates else None)
                        if chosen:
                            file_local_ip = chosen
                            known_ips.add(chosen)
                        else:
                            logger.error("No non-loopback addresses found in my_addresses for %s: %s", path, addrs)
                            raise ValueError(f"No non-loopback addresses found in my_addresses for {path}: {addrs}")
                    # continue scanning; spans may come later
                    continue
                parsed = parse_json_span_event(obj)
                if not parsed:
                    continue
                msg, data = parsed
                ts_raw = str(data.get("ts", ""))
                ts = parse_json_ts(ts_raw)
                if ts is None:
                    logger.error("Unparseable timestamp '%s' in %s; defaulting to epoch", ts_raw, path)
                    ts = dt.datetime.fromtimestamp(0)
                # Fields from flattened span dict
                name = str(data.get("span_name", data.get("name", "")))
                kind = str(data.get("kind", "")).upper()
                tid = str(data.get("trace_id", ""))
                sid = str(data.get("span_id", ""))
                psid = data.get("parent_span_id")
                key = (tid, sid)

                if msg == "span_begin":
                    if not tid or not sid or not name or not kind:
                        logger.error(
                            "Malformed span_begin in %s (missing fields): tid=%s sid=%s name=%s kind=%s",
                            path, tid, sid, name, kind,
                        )
                        raise ValueError(f"Malformed span_begin in {path} (missing fields): tid={tid} sid={sid} name={name} kind={kind}")
                    if kind not in {"SERVER", "CLIENT"}:
                        logger.error(
                            "Unexpected span kind in span_begin: kind=%s tid=%s sid=%s name=%s (file=%s)",
                            kind, tid, sid, name, path,
                        )
                        raise ValueError(f"Unexpected span kind in span_begin: kind={kind} tid={tid} sid={sid} name={name} (file={path})")
                    sp = Span(
                        trace_id=tid,
                        span_id=sid,
                        parent_span_id=str(psid) if psid else None,
                        kind=kind,
                        name=name,
                        start_ts=ts,
                        local_label=local_label,
                        local_ip=file_local_ip,
                        local_file=path,
                    )
                    # Capture structured fields when present
                    sp.http_method = str(data.get("http.method")) if data.get("http.method") is not None else sp.http_method
                    sp.http_path = str(data.get("http.path")) if data.get("http.path") is not None else sp.http_path
                    sp.http_url = str(data.get("http.url")) if data.get("http.url") is not None else sp.http_url
                    sp.client_ip = str(data.get("client.ip")) if data.get("client.ip") is not None else sp.client_ip
                    # Status code if present
                    if data.get("http.status_code") is not None:
                        try:
                            sp.status_code = int(data.get("http.status_code"))
                        except Exception:
                            pass
                    if data.get("request_is_sync") is not None:
                        try:
                            sp.request_is_sync = bool(data.get("request_is_sync"))
                        except Exception:
                            pass
                    spans_by_key[key] = sp
                    collect_known_ips_from_span(sp, known_ips)
                elif msg == "span_end":
                    sp = spans_by_key.get(key)
                    if sp is None:
                        # Create minimal span if begin was not seen in provided files
                        logger.error(
                            "span_end without matching span_begin (tid=%s sid=%s) in %s; creating synthetic span",
                            tid, sid, path,
                        )
                        sp = Span(
                            trace_id=tid,
                            span_id=sid or f"unknown_{len(spans_by_key)}",
                            parent_span_id=str(psid) if psid else None,
                            kind=kind or "SERVER",
                            name=name,
                            start_ts=ts,
                            local_label=local_label,
                            local_ip=file_local_ip,
                            local_file=path,
                        )
                    spans_by_key[key] = sp
                    collect_known_ips_from_span(sp, known_ips)
                    # Update structured fields from end event (often richer)
                    try:
                        if data.get("http.method") is not None:
                            sp.http_method = str(data.get("http.method"))
                        if data.get("http.path") is not None:
                            sp.http_path = str(data.get("http.path"))
                        if data.get("http.url") is not None:
                            sp.http_url = str(data.get("http.url"))
                        if data.get("client.ip") is not None:
                            sp.client_ip = str(data.get("client.ip"))
                        if data.get("http.status_code") is not None:
                            try:
                                sp.status_code = int(data.get("http.status_code"))
                            except Exception:
                                pass
                        if data.get("request_is_sync") is not None:
                            sp.request_is_sync = bool(data.get("request_is_sync"))
                    except Exception:
                        pass
                    # Duration may be present as duration_ms attribute in JSON; otherwise leave None
                    dur = data.get("duration_ms")
                    try:
                        sp.duration_ms = float(dur) if dur is not None else sp.duration_ms
                        if sp.duration_ms is not None and sp.duration_ms < 0:
                            logger.warning(
                                "Negative duration_ms for span: tid=%s sid=%s duration_ms=%s",
                                sp.trace_id, sp.span_id, sp.duration_ms,
                            )
                    except Exception:
                        pass
                    # Prefer explicit outbound request duration if present
                    try:
                        req_dur = data.get("request_duration_ms")
                        if req_dur is not None:
                            sp.request_duration_ms = float(req_dur)
                            if sp.request_duration_ms < 0:
                                logger.warning(
                                    "Negative request_duration_ms for CLIENT span: tid=%s sid=%s duration_ms=%s",
                                    sp.trace_id, sp.span_id, sp.request_duration_ms,
                                )
                    except Exception:
                        pass
        # Retroactively assign local_ip to any earlier spans from this file that were created
        # before the 'my_addresses' record was seen
        if file_local_ip:
            for sp in spans_by_key.values():
                if sp.local_file == path and sp.local_ip is None:
                    sp.local_ip = file_local_ip
        else:
            logger.error("Did not see my_addresses for file %s; local_ip unknown for its spans", path)
    # Sort spans by start_ts
    spans = sorted(spans_by_key.values(), key=lambda s: s.start_ts)
    # Post-parse validation: check unresolved parent_span_id references
    by_trace: Dict[str, set[str]] = {}
    for sp in spans:
        by_trace.setdefault(sp.trace_id, set()).add(sp.span_id)
    for sp in spans:
        if sp.parent_span_id and sp.parent_span_id not in by_trace.get(sp.trace_id, set()):
            logger.error(
                "Unresolved parent_span_id: tid=%s sid=%s parent_sid=%s (no matching span_begin/end in inputs)",
                sp.trace_id, sp.span_id, sp.parent_span_id,
            )
    return spans, sorted(known_ips)


def normalize_hostname_to_ip(host: str, known_ips: List[str], host_map: Dict[str, str]) -> str:
    # If host contains a dashed IP that matches a known dotted IP, replace with that IP
    # First, use explicit host->ip mappings from the log
    if host in host_map:
        return host_map[host]
    # Prefer explicit matches against our known IPs (dotted or dashed forms)
    for ip in known_ips:
        dashed_ip = ip.replace('.', '-')
        if dashed_ip in host or ip in host:
            return ip
    # If the hostname includes a dashed-quad anywhere, extract and return only the dotted IP
    m = DASHED_IP_RE.search(host)
    if m:
        dashed = m.group("dashed")
        dotted = dashed.replace("-", ".")
        return dotted
    # If the hostname includes a dotted-quad anywhere, return that IP
    m2 = re.search(r"\b(\d{1,3}(?:\.\d{1,3}){3})\b", host)
    if m2:
        return m2.group(1)
    return host


def build_plantuml(spans: List[Span], filter_trace: Optional[str], known_ips: List[str], ip_alias_map: Dict[str, str]) -> str:
    lines: List[str] = []
    logged_replacements: set[Tuple[str, str]] = set()  # (original, replaced)
    lines.append("@startuml")
    # Collect participants from spans
    participants: Dict[str, str] = {}  # alias -> label

    msgs: List[Message] = []
    directives: List[Directive] = []
    unaliased_ips_logged: set[str] = set()

    # Build a per-file preferred local label from alias map by looking at peers in the same file
    file_preferred_label: Dict[str, str] = {}
    file_label_counts: Dict[str, Dict[str, int]] = {}
    for sp in spans:
        fname = sp.local_file or ""
        if not fname:
            continue
        # Consider both SERVER remote and CLIENT host using structured attributes
        if sp.kind == "SERVER" and sp.client_ip:
            norm = normalize_hostname_to_ip(sp.client_ip, known_ips, {})
            if norm in ip_alias_map:
                label = ip_alias_map[norm]
                file_label_counts.setdefault(fname, {}).setdefault(label, 0)
                file_label_counts[fname][label] += 1
        elif sp.kind == "CLIENT" and sp.http_url:
            try:
                host = urlparse(sp.http_url).hostname or ""
            except Exception:
                host = ""
            if host:
                norm = normalize_hostname_to_ip(host, known_ips, {})
                if norm in ip_alias_map:
                    label = ip_alias_map[norm]
                    file_label_counts.setdefault(fname, {}).setdefault(label, 0)
                    file_label_counts[fname][label] += 1
    for fname, counts in file_label_counts.items():
        best_label = max(counts.items(), key=lambda kv: kv[1])[0]
        file_preferred_label[fname] = best_label

    for sp in spans:
        if filter_trace and sp.trace_id != filter_trace:
            continue
        # Compute depth (nesting) via parent chain length within the same trace
        depth = 0
        try:
            seen = set()
            cur = sp.parent_span_id
            while cur:
                if (sp.trace_id, cur) in seen:
                    break
                seen.add((sp.trace_id, cur))
                depth += 1
                parent = next((p for p in spans if p.trace_id == sp.trace_id and p.span_id == cur), None)
                cur = parent.parent_span_id if parent else None
        except Exception:
            depth = 0
        color = _hex_color_for_trace(sp.trace_id, depth)
        trace_tag = f"[{sp.trace_id[:4]}-{sp.span_id[:2]}]"
        time_str = sp.start_ts.strftime("%H:%M:%S")
        # Prefer explicit outbound request duration for CLIENT spans if present
        if sp.kind == "CLIENT" and sp.request_duration_ms is not None:
            dur_str = f" ({_fmt_secs(sp.request_duration_ms)})"
        else:
            dur_str = f" ({_fmt_secs(sp.duration_ms)})" if sp.duration_ms is not None else ""

        if sp.kind == "SERVER":
            # Structured attributes required: client_ip, http_method, http_path
            remote = sp.client_ip
            method = sp.http_method
            path = sp.http_path
            if not (remote and method and path):
                logger.warning(
                    "Skipping SERVER span due to missing structured fields: tid=%s sid=%s name=%s client.ip=%s http.method=%s http.path=%s",
                    sp.trace_id, sp.span_id, sp.name, sp.client_ip, sp.http_method, sp.http_path,
                )
                continue
            label = f"{method} {_normalize_cmapi_path(path)}"
            # Server status code
            code_str = f" [{sp.status_code}]" if sp.status_code is not None else ""
            if sp.status_code is None:
                logger.warning(
                    "Missing http.status_code for SERVER span at render time: tid=%s sid=%s method=%s path=%s",
                    sp.trace_id, sp.span_id, method, path,
                )
            # Replace loopback with file's real IP if available
            if remote in {"127.0.0.1", "::1"} and sp.local_ip:
                orig = remote
                remote = sp.local_ip
                key = (orig, remote)
                if key not in logged_replacements:
                    file_label = os.path.basename(sp.local_file) if sp.local_file else "?"
                    logger.info("Loopback remapped (SERVER): %s -> %s [file=%s]", orig, remote, file_label)
                    logged_replacements.add(key)
            # do not filter self traffic
            # If remote is an IP, keep as-is; else use hostname
            # apply aliasing for IPs
            # Log unaliased IPs
            if is_ip(remote) and remote not in ip_alias_map and remote not in unaliased_ips_logged:
                logger.info("Unaliased IP encountered (SERVER remote): %s. Consider providing --alias %s=NAME", remote, remote)
                unaliased_ips_logged.add(remote)
            remote_label = ip_alias_map.get(remote, remote)
            remote_alias = normalize_participant(remote_label)
            participants.setdefault(remote_alias, remote_label)
            # Prefer local IP alias (e.g., mcs1); otherwise use the local IP string.
            if sp.local_ip:
                if is_ip(sp.local_ip) and sp.local_ip not in ip_alias_map and sp.local_ip not in unaliased_ips_logged:
                    logger.info("Unaliased IP encountered (SERVER local): %s. Consider providing --alias %s=NAME", sp.local_ip, sp.local_ip)
                    unaliased_ips_logged.add(sp.local_ip)
                local_label_effective = ip_alias_map.get(sp.local_ip, sp.local_ip)
            else:
                # As a last resort, use filename label
                local_label_effective = sp.local_label
            local_alias = normalize_participant(local_label_effective)
            participants.setdefault(local_alias, local_label_effective)
            arrow = _colorized_arrow('->', color)  # SERVER handling is synchronous from remote to local
            msgs.append(Message(sp.start_ts, remote_alias, local_alias, f"{trace_tag} [{time_str}] {label}{dur_str}{code_str}", arrow))
            # Add a note for the request with trace tag and start timestamp
            directives.append(Directive(sp.start_ts, f"note over {remote_alias}, {local_alias}: {trace_tag} [{time_str}]"))
            # Activate server processing lifeline only if duration is known and >= threshold
            if sp.duration_ms is not None and sp.duration_ms >= ACTIVATION_MIN_MS:
                directives.append(Directive(sp.start_ts, f"activate {local_alias} {color}"))
                try:
                    srv_end = sp.start_ts + dt.timedelta(milliseconds=sp.duration_ms)
                    directives.append(Directive(srv_end, f"deactivate {local_alias}"))
                except Exception:
                    pass
            elif sp.duration_ms is None:
                # Log that we couldn't determine server processing duration
                logger.warning(
                    "No duration for SERVER span: tid=%s sid=%s method=%s path=%s local=%s",
                    sp.trace_id, sp.span_id, method, path, local_label_effective,
                )
        elif sp.kind == "CLIENT":
            # Structured attributes required: http_url, http_method
            if not (sp.http_url and sp.http_method):
                logger.warning(
                    "Skipping CLIENT span due to missing structured fields: tid=%s sid=%s name=%s http.url=%s http.method=%s",
                    sp.trace_id, sp.span_id, sp.name, sp.http_url, sp.http_method,
                )
                continue
            # Extract host and path from URL
            try:
                p = urlparse(sp.http_url)
                host = p.hostname or sp.http_url
                path = p.path or "/"
            except Exception:
                logger.warning("Failed to parse URL for CLIENT span (tid=%s sid=%s): %s", sp.trace_id, sp.span_id, sp.http_url)
                host = sp.http_url
                path = sp.http_url
            label = f"{sp.http_method} {_normalize_cmapi_path(path)}"
            # Replace loopback with file's real IP if available
            if host in {"127.0.0.1", "::1"} and sp.local_ip:
                orig = host
                host = sp.local_ip
                key = (orig, host)
                if key not in logged_replacements:
                    file_label = os.path.basename(sp.local_file) if sp.local_file else "?"
                    logger.info("Loopback remapped (CLIENT): %s -> %s [file=%s]", orig, host, file_label)
                    logged_replacements.add(key)
            # Normalize hostnames that embed an IP in dashed form to dotted IP when recognized
            new_host = normalize_hostname_to_ip(host, known_ips, {})
            if new_host != host:
                key = (host, new_host)
                if key not in logged_replacements:
                    file_label = os.path.basename(sp.local_file) if sp.local_file else "?"
                    logger.info("Host normalized: %s -> %s [file=%s]", host, new_host, file_label)
                    logged_replacements.add(key)
            host = new_host
            # do not filter self traffic
            # If host resolved to IP, allow user alias to apply
            if is_ip(host) and host not in ip_alias_map and host not in unaliased_ips_logged:
                logger.info("Unaliased IP encountered (CLIENT host): %s. Consider providing --alias %s=NAME", host, host)
                unaliased_ips_logged.add(host)
            remote_label = ip_alias_map.get(host, host)
            remote_alias = normalize_participant(remote_label)
            participants.setdefault(remote_alias, remote_label)
            # Prefer local IP alias (e.g., mcs1); otherwise use the local IP string.
            if sp.local_ip:
                if is_ip(sp.local_ip) and sp.local_ip not in ip_alias_map and sp.local_ip not in unaliased_ips_logged:
                    logger.info("Unaliased IP encountered (CLIENT local): %s. Consider providing --alias %s=NAME", sp.local_ip, sp.local_ip)
                    unaliased_ips_logged.add(sp.local_ip)
                local_label_effective = ip_alias_map.get(sp.local_ip, sp.local_ip)
            else:
                # As a last resort, use filename label
                local_label_effective = sp.local_label
            local_alias = normalize_participant(local_label_effective)
            participants.setdefault(local_alias, local_label_effective)
            # Use async arrow for async requests
            is_sync = sp.request_is_sync
            arrow = _colorized_arrow('->' if (is_sync is True) else '->>' if (is_sync is False) else '->', color)
            msgs.append(Message(sp.start_ts, local_alias, remote_alias, f"{trace_tag} [{time_str}] {label}{dur_str}", arrow))
            # Add a response arrow back at end time if we know duration
            end_ts: Optional[dt.datetime] = None
            if sp.request_duration_ms is not None:
                try:
                    end_ts = sp.start_ts + dt.timedelta(milliseconds=sp.request_duration_ms)
                except Exception:
                    end_ts = None
            elif sp.duration_ms is not None:
                try:
                    end_ts = sp.start_ts + dt.timedelta(milliseconds=sp.duration_ms)
                except Exception:
                    end_ts = None
            if end_ts is not None:
                end_time_str = end_ts.strftime("%H:%M:%S")
                resp_arrow = '-->' if (is_sync is True or is_sync is None) else '-->>'
                # Build details once
                dur_val = None
                if sp.request_duration_ms is not None:
                    dur_val = sp.request_duration_ms
                elif sp.duration_ms is not None:
                    dur_val = sp.duration_ms
                same_second = (end_time_str == time_str)
                # Response label: always include method/path; timestamp first
                resp_label_parts = ["Response", f"[{end_time_str}]", label]
                if sp.status_code is not None:
                    code_token = str(sp.status_code)
                    if sp.status_code != 200:
                        code_token = f"<color:red>{code_token}</color>"
                    resp_label_parts.append(f"[{code_token}]")
                else:
                    logger.error(
                        "Missing status_code for CLIENT response: tid=%s sid=%s url=%s",
                        sp.trace_id, sp.span_id, sp.http_url,
                    )
                resp_label = ' '.join(resp_label_parts)
                msgs.append(Message(end_ts, remote_alias, local_alias, f"{trace_tag} {resp_label}", _colorized_arrow(resp_arrow, color)))
                if same_second:
                    # Single combined note at the (end) time with tag and duration only
                    parts = [f"{trace_tag}"]
                    if dur_val is not None:
                        parts.append(f"dur={_fmt_secs(dur_val)}")
                    note_text = f"note over {local_alias}, {remote_alias}: " + ", ".join(parts)
                    directives.append(Directive(end_ts, note_text))
                else:
                    # Two separate notes: request note at start, response note at end
                    directives.append(Directive(sp.start_ts, f"note over {local_alias}, {remote_alias}: {trace_tag} [{time_str}]"))
                    parts = [f"{trace_tag}"]
                    if dur_val is not None:
                        parts.append(f"dur={_fmt_secs(dur_val)}")
                    note_text = f"note over {remote_alias}, {local_alias}: " + ", ".join(parts)
                    directives.append(Directive(end_ts, note_text))
            else:
                # No end time available; emit only the request note at start
                directives.append(Directive(sp.start_ts, f"note over {local_alias}, {remote_alias}: {trace_tag} [{time_str}]"))
                logger.info(
                    "No duration for CLIENT request: tid=%s sid=%s method=%s url=%s",
                    sp.trace_id, sp.span_id, sp.http_method, sp.http_url,
                )

    # Emit participants sorted by label (case-insensitive)
    items = list(participants.items())  # (alias, label)
    items.sort(key=lambda x: x[1].lower())

    def emit_part(alias: str, label: str):
        if alias == label and alias.isidentifier():
            lines.append(f"participant {alias}")
        else:
            lines.append(f"participant \"{label}\" as {alias}")

    for alias, label in items:
        emit_part(alias, label)

    # Sort and emit messages and directives merged by time
    msgs.sort(key=lambda m: m.ts)
    directives.sort(key=lambda d: d.ts)
    i = j = 0
    while i < len(msgs) or j < len(directives):
        if j >= len(directives) or (i < len(msgs) and msgs[i].ts <= directives[j].ts):
            m = msgs[i]
            lines.append(f"{m.src} {m.arrow} {m.dst} : {m.label}")
            i += 1
        else:
            d = directives[j]
            lines.append(d.text)
            j += 1

    lines.append("@enduml")
    return "\n".join(lines) + "\n"


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Convert CMAPI tracing JSONL logs to PlantUML sequence diagram")
    ap.add_argument("paths", nargs="+", help="One or more JSONL log files.")
    ap.add_argument("--filter-trace", help="Only include spans for the given trace_id (tid)")
    ap.add_argument("--alias", action="append", default=[], metavar="IP=NAME", help="Assign a label NAME to IP. May be used multiple times.")
    ap.add_argument("--list-ips", action="store_true", help="Print only the set of IP addresses encountered and exit")
    ap.add_argument("--output", default="diagram.puml", help="Output PlantUML path (default: diagram.puml)")
    args = ap.parse_args(argv)

    if args.list_ips:
        logs = args.paths
        if not logs:
            ap.error("At least one log path is required with --list-ips")
        spans, known_ips = parse_jsonl_logs(logs)
    else:
        # Determine logs and output path
        logs = args.paths
        output_path = args.output
        if not logs:
            ap.error("Provide at least one JSONL log path")
        spans, known_ips = parse_jsonl_logs(logs)

    # Build alias map from args.alias entries (used by both list-ips and diagram output)
    ip_alias_map: Dict[str, str] = {}
    for item in args.alias:
        if "=" in item:
            ip, name = item.split("=", 1)
            ip = ip.strip()
            name = name.strip()
            if ip:
                ip_alias_map[ip] = name
        else:
            logger.error("Invalid alias format: %s", item)
            return 2

    if args.list_ips:
        to_print = sorted(set(known_ips))
        for ip in to_print:
            parts = [ip]
            if ip in ip_alias_map:
                parts.append(f"alias={ip_alias_map[ip]}")
            print(" ".join(parts))
        return 0


    puml = build_plantuml(spans, args.filter_trace, known_ips, ip_alias_map)

    if args.list_ips:
        # Already handled above, but keep structure consistent
        return 0

    # Write to output file
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(puml)
    logger.info("Diagram written to %s", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
