#!/usr/bin/env python3
"""Create/update Jira tickets from ColumnStore Infer (SAST) findings.

Target: the MCOL project on jira.mariadb.org (Jira Server/Data Center,
REST API v2), authenticated with a Personal Access Token (PAT).

See MCOL-6494 for the full design and history. Commands:

  check-connection      read-only auth/permissions check (no writes)
  describe              what the project's create screen / workflow accept
  parse   --report R    parse an Infer report.json into groups (no Jira)
  sync    --report R    create/update one ticket per (component, bug_type)
                        group, suggest closing tickets whose group vanished;
                        --dry-run first-class, --groups-file for test input
  cleanup --yes         delete (or close, if delete is denied) all tickets
                        carrying --label; refuses on the production label

The nightly flow is: run_infer.sh produces report.json -> sync --report
turns it into MCOL tickets (label `cs-infer`, security level Company
internal, never auto-closing anything).

Ticket identity is the *key label* `<label>-key-<component>-<bug_type>`, so
re-runs find the existing ticket by an exact JQL label match instead of a
fuzzy summary match. The description holds the current state (the affected
`file:line` list) and is only rewritten when it actually changed, so a
no-op re-run performs no writes at all.

Auth / configuration (never hard-coded, never committed):
  JIRA_URL    e.g. https://jira.mariadb.org   (--url overrides)
  JIRA_TOKEN  the PAT                          (--token-file overrides)

The PAT must NOT be committed. Keep it in an env var or a file outside the
repo (chmod 600); in CI it comes from Drone secrets.
"""

import argparse
import json
import os
import re
import sys
import time

try:
    import requests
except ImportError:
    sys.exit("error: the 'requests' library is required (pip install requests)")


# --------------------------------------------------------------------------
# Conventions (deliberately distinct from the security team's audit flow)
# --------------------------------------------------------------------------
# Production label; iterations 1-4 run under PROD_LABEL + "-test" instead so
# every experimental ticket is trivially found and deleted again.
PROD_LABEL = "cs-infer"
DEFAULT_LABEL = "cs-infer-test"
# The corporate SAST process (Confluence: "SAST Scanning & Finding") requires
# actionable findings to carry this label. Test runs get "-test" appended so
# throwaway tickets never pollute the security team's audit queries.
AUDIT_LABEL = "sast-finding"
DEFAULT_ASSIGNEE = "stasusov"  # Stanislav Usov
DEFAULT_ISSUE_TYPE = "Bug"
DEFAULT_PROJECT = "MCOL"
# Findings describe potential vulnerabilities, so tickets must not be public
# (see MCOL-6423 for the shape we are copying). Cleared with
# --no-security-level only.
DEFAULT_SECURITY_LEVEL = "Company internal"
# Fixed target per the integration plan; can still be overridden via --url/env.
DEFAULT_JIRA_URL = "https://jira.mariadb.org"

# Keep descriptions inside Jira's field limit; iteration 6 will attach the
# full list instead of truncating.
MAX_LOCATIONS_IN_DESCRIPTION = 200

# A finding group == one ticket. Iteration 4 replaces this hardcoded input
# with a parsed report.json plus the path -> component mapping; until then
# --groups-file can point at a JSON file with this same shape.
HARDCODED_GROUPS = [
    {
        "component": "FuncExp",
        "bug_type": "USE_AFTER_LIFETIME",
        "locations": [
            "storage/columnstore/columnstore/utils/funcexp/func_substr.cpp:118",
            "storage/columnstore/columnstore/utils/funcexp/func_concat.cpp:74",
        ],
    },
    {
        "component": "FuncExp",
        "bug_type": "NULL_DEREFERENCE",
        "locations": [
            "storage/columnstore/columnstore/utils/funcexp/functor_str.cpp:52",
        ],
    },
    {
        "component": "PrimProc",
        "bug_type": "DEAD_STORE",
        "locations": [
            "storage/columnstore/columnstore/primitives/linux-port/column.cpp:1204",
            "storage/columnstore/columnstore/primitives/primproc/batchprimitiveprocessor.cpp:880",
        ],
    },
]


# --------------------------------------------------------------------------
# Infer report parsing + path -> component mapping (iteration 4)
# --------------------------------------------------------------------------
# The ColumnStore checkout lives here inside the server tree; report.json
# paths are relative to the server root.
CS_ROOT = "storage/columnstore/columnstore/"

# Path prefix (relative to CS_ROOT) -> MCOL component; the longest matching
# prefix wins. Anything unmapped falls through to CATCH_ALL_COMPONENT with a
# warning. Rows marked "(confirm)" in the plan are kept in one reviewed
# place: here.
CATCH_ALL_COMPONENT = "N/A"
COMPONENT_MAP = {
    "dbcon/mysql/": "MDB Plugin",
    "dbcon/ddlpackage/": "DDLProc",
    "dbcon/ddlpackageproc/": "DDLProc",
    "ddlproc/": "DDLProc",
    "dbcon/dmlpackage/": "DMLProc",
    "dbcon/dmlpackageproc/": "DMLProc",
    "dmlproc/": "DMLProc",
    "dbcon/joblist/": "ExeMgr",       # (confirm)
    "dbcon/execplan/": "ExeMgr",      # (confirm)
    "dbcon/rbo/": "ExeMgr",           # (confirm)
    "primitives/": "PrimProc",
    "writeengine/": "writeengine",
    "storage-manager/": "Storage Manager",
    "utils/cloudio/": "Storage Manager",
    "oam/": "ProcMgr",                # (confirm)
    "utils/funcexp/": "FuncExp",
    "cmapi/": "cmapi",
    "tools/": "cpimport",             # (confirm)
    # versioning/BRM has no MCOL component -> catch-all, per the plan
}


def map_component(file_path, unmapped):
    """The MCOL component for a report.json file path.

    `unmapped` collects catch-all prefixes -> hit counts for warning output.
    """
    idx = file_path.find(CS_ROOT)
    if idx < 0:
        # Under storage/columnstore/ but not the columnstore submodule
        # (e.g. libmarias3): nothing sensible to map to.
        unmapped["<outside columnstore submodule>"] = (
            unmapped.get("<outside columnstore submodule>", 0) + 1)
        return CATCH_ALL_COMPONENT
    rel = file_path[idx + len(CS_ROOT):]
    best = ""
    for prefix in COMPONENT_MAP:
        if rel.startswith(prefix) and len(prefix) > len(best):
            best = prefix
    if best:
        return COMPONENT_MAP[best]
    top = rel.split("/", 1)[0] + "/" if "/" in rel else rel
    unmapped[top] = unmapped.get(top, 0) + 1
    return CATCH_ALL_COMPONENT


def parse_report(path, include_suppressed=False):
    """Turn an Infer report.json into finding groups.

    Mirrors the run_infer.sh gate exactly: keep findings under
    storage/columnstore/ that are neither censored (config-driven) nor
    suppressed (`// @infer-ignore`, unless include_suppressed -- a TEST
    knob; suppressed findings are never ticketed in production).

    Returns (groups, stats, unmapped).
    """
    try:
        with open(os.path.expanduser(path), "r", encoding="utf-8") as fh:
            report = json.load(fh)
    except (OSError, ValueError) as exc:
        raise JiraError(f"cannot read --report {path!r}: {exc}")
    if not isinstance(report, list):
        raise JiraError(f"{path}: expected a JSON list (an Infer report.json)")

    stats = {"total": len(report), "outside": 0, "censored": 0,
             "suppressed": 0, "picked": 0}
    unmapped = {}
    buckets = {}
    for finding in report:
        file_path = finding.get("file", "")
        if "storage/columnstore/" not in file_path:
            stats["outside"] += 1
            continue
        if "censored_reason" in finding:      # key presence, like the jq gate
            stats["censored"] += 1
            continue
        if finding.get("suppressed") is True and not include_suppressed:
            stats["suppressed"] += 1
            continue
        stats["picked"] += 1
        component = map_component(file_path, unmapped)
        key = (component, finding.get("bug_type", "UNKNOWN"))
        buckets.setdefault(key, set()).add(f"{file_path}:{finding.get('line', 0)}")

    groups = [
        {"component": component, "bug_type": bug_type, "locations": sorted(locations)}
        for (component, bug_type), locations in sorted(buckets.items())
    ]
    return groups, stats, unmapped


def report_groups(args):
    """Parse args.report into groups, printing stats + unmapped warnings."""
    groups, stats, unmapped = parse_report(args.report, args.include_suppressed)
    print(
        f"Report        : {args.report}\n"
        f"  findings    : {stats['total']} total, {stats['outside']} outside "
        f"ColumnStore, {stats['censored']} censored, "
        f"{stats['suppressed']} suppressed, {stats['picked']} ticket-worthy"
        + ("  [INCLUDING SUPPRESSED -- test only!]" if args.include_suppressed else "")
    )
    for prefix, hits in sorted(unmapped.items()):
        print(
            f"  WARNING   : unmapped path prefix {prefix!r} ({hits} finding(s)) "
            f"-> component {CATCH_ALL_COMPONENT!r}",
            file=sys.stderr,
        )
    return groups


# --------------------------------------------------------------------------
# Jira Server/DC REST v2 client
# --------------------------------------------------------------------------
class JiraError(RuntimeError):
    """A Jira REST call failed. `status` is the HTTP code when there was one."""

    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


class JiraClient:
    """Minimal Jira Server/DC REST v2 client using PAT (Bearer) auth."""

    # Retry these transient statuses with exponential backoff.
    RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})

    def __init__(self, base_url, token, timeout=30, max_retries=4):
        if not base_url:
            raise JiraError("no Jira base URL (set JIRA_URL or pass --url)")
        if not token:
            raise JiraError("no Jira token (set JIRA_TOKEN or pass --token-file)")
        self.base_url = base_url.rstrip("/")
        self.api = f"{self.base_url}/rest/api/2"
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    def _request(self, method, path, **kwargs):
        url = path if path.startswith("http") else f"{self.api}{path}"
        kwargs.setdefault("timeout", self.timeout)
        last_exc = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = self.session.request(method, url, **kwargs)
            except requests.RequestException as exc:
                last_exc = exc
                resp = None
            else:
                if resp.status_code not in self.RETRY_STATUSES:
                    if resp.status_code >= 400:
                        raise JiraError(
                            f"{method} {url} -> HTTP {resp.status_code}: "
                            f"{resp.text[:500]}",
                            status=resp.status_code,
                        )
                    return resp
            # transient failure -> back off (0.5, 1, 2, 4 ... seconds)
            if attempt < self.max_retries:
                backoff = 0.5 * (2 ** attempt)
                where = f"HTTP {resp.status_code}" if resp is not None else str(last_exc)
                print(
                    f"  (transient {where}; retry {attempt + 1}/"
                    f"{self.max_retries} in {backoff:.1f}s)",
                    file=sys.stderr,
                )
                time.sleep(backoff)
        if last_exc is not None:
            raise JiraError(f"{method} {url} failed after retries: {last_exc}")
        raise JiraError(
            f"{method} {url} still failing after {self.max_retries} retries "
            f"(HTTP {resp.status_code})"
        )

    def get_json(self, path, **kwargs):
        return self._request("GET", path, **kwargs).json()

    # -- read-only helpers -------------------------------------------------
    def myself(self):
        """The authenticated user (validates the token)."""
        return self.get_json("/myself")

    def get_project(self, key):
        return self.get_json(f"/project/{key}")

    def get_components(self, key):
        return self.get_json(f"/project/{key}/components")

    def get_versions(self, key):
        return self.get_json(f"/project/{key}/versions")

    def count(self, jql):
        """How many issues match `jql` (fetches no issues)."""
        return self.get_json(
            "/search", params={"jql": jql, "maxResults": 0}
        ).get("total", 0)

    def search(self, jql, limit=None, page_size=50,
               fields=("summary", "status", "labels")):
        """Run a JQL search.

        Pages until the result set is exhausted, or until `limit` issues have
        been collected. Always pass a `limit` for broad queries -- an
        unbounded search over a whole project is thousands of requests.
        """
        issues = []
        start_at = 0
        while True:
            want = page_size if limit is None else min(page_size, limit - len(issues))
            params = {
                "jql": jql,
                "startAt": start_at,
                "maxResults": want,
                "fields": ",".join(fields),
            }
            page = self.get_json("/search", params=params)
            batch = page.get("issues", [])
            issues.extend(batch)
            start_at += len(batch)
            if (
                not batch
                or (limit is not None and len(issues) >= limit)
                or start_at >= page.get("total", 0)
            ):
                return issues

    def get_issue(self, key,
                  fields=("summary", "description", "labels", "status", "security")):
        return self.get_json(f"/issue/{key}", params={"fields": ",".join(fields)})

    def get_create_meta(self, project, issue_type):
        return self.get_json(
            "/issue/createmeta",
            params={
                "projectKeys": project,
                "issuetypeNames": issue_type,
                "expand": "projects.issuetypes.fields",
            },
        )

    # -- write helpers -----------------------------------------------------
    def create_issue(self, fields):
        return self._request("POST", "/issue", json={"fields": fields}).json()

    def update_issue(self, key, fields):
        self._request("PUT", f"/issue/{key}", json={"fields": fields})

    def add_comment(self, key, body):
        return self._request("POST", f"/issue/{key}/comment", json={"body": body}).json()

    def add_issue_link(self, type_name, inward_key, outward_key):
        self._request("POST", "/issueLink", json={
            "type": {"name": type_name},
            "inwardIssue": {"key": inward_key},
            "outwardIssue": {"key": outward_key},
        })

    def delete_issue(self, key):
        self._request("DELETE", f"/issue/{key}")

    def get_transitions(self, key):
        """Transitions available to us, with their transition-screen fields."""
        return self.get_json(
            f"/issue/{key}/transitions", params={"expand": "transitions.fields"}
        ).get("transitions", [])

    def transition_issue(self, key, transition_id, fields=None):
        payload = {"transition": {"id": transition_id}}
        if fields:
            payload["fields"] = fields
        self._request("POST", f"/issue/{key}/transitions", json=payload)


# --------------------------------------------------------------------------
# Ticket identity, summary and description
# --------------------------------------------------------------------------
def slugify(text):
    """Lowercase, label-safe slug (Jira labels may not contain spaces)."""
    slug = re.sub(r"[^a-z0-9]+", "-", str(text).lower())
    return slug.strip("-")


def key_label(base_label, group):
    """The machine key that gives a ticket its identity: (component, bug_type)."""
    return f"{base_label}-key-{slugify(group['component'])}-{slugify(group['bug_type'])}"


def audit_label(base_label):
    return AUDIT_LABEL if base_label == PROD_LABEL else f"{AUDIT_LABEL}-test"


def ticket_labels(base_label, group):
    return [base_label, audit_label(base_label), key_label(base_label, group)]


def ticket_summary(base_label, group):
    prefix = "[CS-Infer][dev]"
    if base_label != PROD_LABEL:
        # Make throwaway tickets obvious to anyone browsing the project.
        prefix = f"[TEST]{prefix}"
    return f"{prefix} {group['bug_type']} in {group['component']}"


def ticket_description(group, branch=None):
    """Description == current state.

    Deliberately free of timestamps or run ids: an unchanged set of findings
    must produce a byte-identical description so a re-run is a true no-op.
    (The branch is fine -- it is constant for this flow.)
    """
    locations = sorted(set(group["locations"]))
    total = len(locations)
    shown = locations[:MAX_LOCATIONS_IN_DESCRIPTION]
    lines = [
        "Filed automatically by the ColumnStore Infer (SAST) scan "
        "(see build/run_infer.sh).",
        "",
        f"*Bug type:* {{{{{group['bug_type']}}}}}",
        f"*Component:* {group['component']}",
    ]
    if branch:
        lines.append(f"*Branch scanned:* {branch}")
    lines.extend([
        "",
        f"*Affected locations ({total}):*",
        "{noformat}",
    ])
    lines.extend(shown)
    if total > len(shown):
        lines.append(f"... and {total - len(shown)} more")
    lines.append("{noformat}")
    lines.extend(
        [
            "",
            "This description is rewritten by every scan and always reflects the "
            "*current* findings. Do not edit it by hand; add comments instead.",
            "",
            "*If a finding is a false positive* (per the SAST Scanning & Finding "
            "process): get your team lead's confirmation, add an in-code "
            "suppression (see https://fbinfer.com/docs/suppressions/) with a "
            "commit message referencing this ticket, and only close the ticket "
            "once the suppression is committed.",
        ]
    )
    return "\n".join(lines)


CREATED_COMMENT = (
    "Ticket opened automatically from the ColumnStore Infer scan. "
    "The description tracks the current set of affected locations."
)

GONE_COMMENT = (
    "The Infer scan no longer reports any findings for this group -- they "
    "may have been fixed. Please verify and close this ticket if so; it is "
    "never closed automatically. If the findings return, the scan comments "
    "again and withdraws this suggestion."
)

REAPPEARED_COMMENT = (
    "The Infer scan reports findings for this group again after they had "
    "disappeared; withdrawing the earlier close suggestion. The description "
    "lists the current locations."
)


def gone_label(base_label):
    """Marks a ticket whose finding group vanished: state lives in Jira, so
    re-runs know the close suggestion was already posted."""
    return f"{base_label}-gone"

# Recovers the location list from a description we wrote on an earlier run:
# Jira itself is the state store, so there is no side file to drift.
_LOCATIONS_RE = re.compile(r"\{noformat\}\n(.*?)\n\{noformat\}", re.S)


def parse_locations(description):
    """The location set recorded in `description`, or None if unknowable.

    Returns None when the description is not ours or was truncated -- in
    that case we cannot tell new findings from old ones and must not guess.
    """
    match = _LOCATIONS_RE.search(description or "")
    if not match:
        return None
    lines = [line.strip() for line in match.group(1).splitlines() if line.strip()]
    if any(line.startswith("... and ") for line in lines):
        return None  # truncated: the recorded state is incomplete
    return set(lines)


def new_locations_comment(group, added):
    listing = "\n".join(sorted(added))
    plural = "s" if len(added) != 1 else ""
    return (
        f"The Infer scan reports {len(added)} new location{plural} for "
        f"{{{{{group['bug_type']}}}}} in {group['component']}:\n"
        f"{{noformat}}\n{listing}\n{{noformat}}\n"
        f"The description lists the full current set."
    )


def load_groups(path):
    """Read finding groups from a JSON file with the HARDCODED_GROUPS shape."""
    try:
        with open(os.path.expanduser(path), "r", encoding="utf-8") as fh:
            groups = json.load(fh)
    except (OSError, ValueError) as exc:
        raise JiraError(f"cannot read --groups-file {path!r}: {exc}")
    if not isinstance(groups, list):
        raise JiraError(f"{path}: expected a JSON list of groups")
    seen = {}
    for index, group in enumerate(groups):
        where = f"{path}[{index}]"
        if not isinstance(group, dict):
            raise JiraError(f"{where}: expected an object")
        for field in ("component", "bug_type", "locations"):
            if field not in group:
                raise JiraError(f"{where}: missing {field!r}")
        if not isinstance(group["locations"], list) or not group["locations"]:
            raise JiraError(f"{where}: 'locations' must be a non-empty list")
        # Two groups with the same identity would fight over one ticket.
        identity = (group["component"], group["bug_type"])
        if identity in seen:
            raise JiraError(
                f"{where}: duplicate group {identity} (also at index {seen[identity]}); "
                f"merge their locations -- both map to the same ticket"
            )
        seen[identity] = index
    return groups


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
def load_token(args):
    """Resolve the PAT from --token-file or the JIRA_TOKEN env var."""
    if args.token_file:
        path = os.path.expanduser(args.token_file)
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return fh.read().strip()
        except OSError as exc:
            raise JiraError(f"cannot read --token-file {path!r}: {exc}")
    return os.environ.get("JIRA_TOKEN", "").strip()


def resolve_url(args):
    return args.url or os.environ.get("JIRA_URL", "").strip() or DEFAULT_JIRA_URL


def make_client(args):
    return JiraClient(resolve_url(args), load_token(args))


# --------------------------------------------------------------------------
# Sync (iteration 1: one hardcoded group)
# --------------------------------------------------------------------------
def find_ticket(client, project, base_label, group):
    """Find the open ticket for this group, matched by the exact key label.

    Returns (issue_or_None, extra_duplicates).
    """
    jql = (
        f'project = {project} AND labels = "{base_label}" '
        f'AND labels = "{key_label(base_label, group)}" '
        f"AND statusCategory != Done ORDER BY created ASC"
    )
    issues = client.search(jql, limit=50)
    if not issues:
        return None, []
    return issues[0], issues[1:]


def ensure_security_level(client, key, level, problems=None):
    """Verify a freshly created ticket really is restricted.

    Jira silently drops fields that are not on the create screen, and a
    ticket full of un-triaged vulnerability findings must not end up public,
    so we check rather than assume -- and shout if we cannot fix it.
    """
    if not level:
        return True
    actual = (client.get_issue(key)["fields"].get("security") or {}).get("name")
    if actual == level:
        print(f"    security  : {level} (verified)")
        return True
    try:
        client.update_issue(key, {"security": {"name": level}})
        actual = (client.get_issue(key)["fields"].get("security") or {}).get("name")
    except JiraError as exc:
        actual = f"<update failed: {exc}>"
    if actual == level:
        print(f"    security  : {level} (set after create)")
        return True
    print(
        f"  *** WARNING: {key} is NOT restricted to {level!r} (currently "
        f"{actual!r}). It may expose findings publicly -- set the security "
        f"level by hand now.",
        file=sys.stderr,
    )
    if problems is not None:
        problems.append(f"{key}: security level not applied")
    return False


def find_predecessors(client, project, base_label, group):
    """Earlier tickets (any status) for this group's key label.

    The SAST process requires new tickets to reference existing issues --
    open, mitigated, or fixed -- to prevent duplicate reviews. Called on
    the create path, where no *open* ticket exists, so these are the
    closed ancestors.
    """
    jql = (
        f'project = {project} AND labels = "{key_label(base_label, group)}" '
        f"ORDER BY created ASC"
    )
    return client.search(jql, limit=20)


def reference_predecessors(client, new_key, predecessors, problems):
    """Link the fresh ticket to its ancestors and say so in a comment."""
    mentions = []
    for old in predecessors:
        status = old["fields"].get("status", {}).get("name", "?")
        mentions.append(f"{old['key']} ({status})")
        try:
            client.add_issue_link("Relates", old["key"], new_key)
        except JiraError as exc:
            print(
                f"  WARNING   : could not link {new_key} to {old['key']}: {exc}",
                file=sys.stderr,
            )
    client.add_comment(
        new_key,
        "This finding group was previously tracked as: " + ", ".join(mentions)
        + ". Review those before starting -- the findings may have "
          "reappeared after a fix or an expired suppression.",
    )
    print(f"  predecessors: {', '.join(mentions)}")


def sync_group(client, group, args, known_components, problems=None):
    """Create the ticket for `group`, or update it if it already exists."""
    labels = ticket_labels(args.label, group)
    summary = ticket_summary(args.label, group)
    description = ticket_description(group, args.branch)
    component = group["component"]

    print(f"\n=== {component} / {group['bug_type']} "
          f"({len(set(group['locations']))} location(s))")
    print(f"  key label : {key_label(args.label, group)}")

    use_component = True
    if known_components is not None and component not in known_components:
        print(
            f"  WARNING   : component {component!r} does not exist in "
            f"{args.project}; creating the ticket without a component",
            file=sys.stderr,
        )
        use_component = False

    existing, duplicates = (None, [])
    if not args.dry_run or args.search_in_dry_run:
        existing, duplicates = find_ticket(client, args.project, args.label, group)
    for dup in duplicates:
        print(
            f"  WARNING   : duplicate open ticket {dup['key']} shares this key "
            f"label; using {existing['key']} and leaving {dup['key']} alone",
            file=sys.stderr,
        )

    if existing is None:
        predecessors = []
        if not args.dry_run or args.search_in_dry_run:
            predecessors = find_predecessors(client, args.project, args.label, group)
        fields = {
            "project": {"key": args.project},
            "issuetype": {"name": args.issue_type},
            "summary": summary,
            "description": description,
            "labels": labels,
        }
        if args.assignee:
            # Only set when given: --assignee '' creates unassigned tickets
            # (sending {"name": ""} would be a 400 from Jira).
            fields["assignee"] = {"name": args.assignee}
        if use_component:
            fields["components"] = [{"name": component}]
        if args.security_level:
            fields["security"] = {"name": args.security_level}
        if args.dry_run:
            print("  action    : CREATE (dry-run, nothing sent)")
            print(f"    summary   : {summary}")
            print(f"    issuetype : {args.issue_type}")
            print(f"    project   : {args.project}")
            print(f"    assignee  : {args.assignee or '(unassigned)'}")
            print(f"    component : {component if use_component else '(none)'}")
            print(f"    security  : {args.security_level or '(none -- ticket will be public!)'}")
            print(f"    labels    : {', '.join(labels)}")
            if predecessors:
                print("    would link to predecessors: "
                      + ", ".join(p["key"] for p in predecessors))
            print("    description:")
            for line in description.splitlines():
                print(f"      | {line}")
            return "create"
        issue = client.create_issue(fields)
        key = issue["key"]
        print(f"  action    : CREATED {key} ({client.base_url}/browse/{key})")
        ensure_security_level(client, key, args.security_level, problems)
        client.add_comment(key, CREATED_COMMENT)
        if predecessors:
            reference_predecessors(client, key, predecessors, problems)
        return "create"

    key = existing["key"]
    current = client.get_issue(key)["fields"]
    changes = {}
    if (current.get("description") or "") != description:
        changes["description"] = description
    if current.get("summary") != summary:
        changes["summary"] = summary
    # Only ever *add* a missing restriction; never relax one someone set.
    current_security = (current.get("security") or {}).get("name")
    if args.security_level and not current_security:
        changes["security"] = {"name": args.security_level}
    current_labels = current.get("labels") or []
    desired_labels = set(current_labels) | set(labels)
    # The group is being reported right now, so any standing close
    # suggestion is obsolete: withdraw the gone marker.
    reappeared = gone_label(args.label) in desired_labels
    if reappeared:
        desired_labels.discard(gone_label(args.label))
    if desired_labels != set(current_labels):
        changes["labels"] = sorted(desired_labels)

    # Which findings are genuinely new since the last run? Only those are
    # worth a notification; anything else is description-as-state churn.
    previous = parse_locations(current.get("description"))
    wanted = set(group["locations"])
    added = sorted(wanted - previous) if previous is not None else []
    removed = sorted(previous - wanted) if previous is not None else []
    if previous is None and changes.get("description"):
        print("  note      : previous description is not parseable "
              "(hand-edited or truncated); skipping the new/gone diff")
    elif added or removed:
        print(f"  delta     : +{len(added)} new, -{len(removed)} no longer reported")

    if not changes:
        print(f"  action    : UNCHANGED {key} (no write)")
        return "unchanged"
    if args.dry_run:
        print(f"  action    : UPDATE {key} (dry-run, nothing sent)")
        for field in sorted(changes):
            print(f"    would update field: {field}")
        if reappeared:
            print("    would comment: findings reappeared, close suggestion withdrawn")
        elif added:
            print(f"    would comment about {len(added)} new location(s)")
        return "update"
    client.update_issue(key, changes)
    print(f"  action    : UPDATED {key} ({', '.join(sorted(changes))})")
    # Comment only on a real transition: findings reappeared or got worse.
    # Removals are silent here; a group disappearing entirely is the
    # disappeared check's job. One comment per run, reappearance first.
    if reappeared:
        client.add_comment(key, REAPPEARED_COMMENT)
        print("  comment   : findings reappeared; close suggestion withdrawn")
    elif added:
        client.add_comment(key, new_locations_comment(group, added))
        print(f"  comment   : {len(added)} new location(s) reported")
    return "update"


def flag_disappeared(client, args, active_keys, problems):
    """Suggest closing open tickets whose finding group vanished.

    Never closes anything. The `-gone` label records that the suggestion
    was already posted, so re-runs stay silent until the group either
    reappears (sync_group withdraws the marker) or someone closes the
    ticket.
    """
    gone = gone_label(args.label)
    prefix = f"{args.label}-key-"
    jql = (
        f'project = {args.project} AND labels = "{args.label}" '
        f"AND statusCategory != Done"
    )
    tally = {"flagged": 0, "already": 0}
    for issue in client.search(jql, limit=500):
        key = issue["key"]
        labels = issue["fields"].get("labels") or []
        key_labels = [lbl for lbl in labels if lbl.startswith(prefix)]
        if not key_labels:
            print(
                f"  WARNING   : {key} carries {args.label!r} but no {prefix}* "
                f"label; cannot match it to a group, leaving it alone",
                file=sys.stderr,
            )
            continue
        if any(k in active_keys for k in key_labels):
            continue  # still reported; sync_group already handled it
        if gone in labels:
            tally["already"] += 1
            print(f"  {key}: still gone, close suggestion already posted (no write)")
            continue
        tally["flagged"] += 1
        if args.dry_run:
            print(f"  {key}: would flag as gone + suggest closing (dry-run)")
            continue
        # Label first (the durable state), then the notification: if the
        # comment fails, a re-run must not re-flag and double-comment.
        client.update_issue(key, {"labels": sorted(set(labels) | {gone})})
        try:
            client.add_comment(key, GONE_COMMENT)
        except JiraError as exc:
            problems.append(f"{key}: flagged as gone, but commenting failed: {exc}")
        print(f"  {key}: group no longer reported -> close suggested")
    return tally


def cmd_sync(args):
    # A plain dry-run touches Jira not at all, so it needs no token either.
    if args.report and args.groups_file:
        raise JiraError("--report and --groups-file are mutually exclusive")
    offline = args.dry_run and not args.search_in_dry_run
    client = None if offline else make_client(args)
    if args.report:
        groups = report_groups(args)
    elif args.groups_file:
        groups = load_groups(args.groups_file)
    else:
        groups = HARDCODED_GROUPS
    mode = "DRY-RUN (no writes)" if args.dry_run else "LIVE"
    print(f"Jira base URL : {resolve_url(args)}"
          + ("   (not contacted: offline dry-run)" if offline else ""))
    if client is not None:
        # Fail fast on a dead PAT. Jira treats a bad Bearer token as an
        # anonymous session: searches then silently miss every
        # Company-internal ticket and creates fail with confusing
        # "field cannot be set" errors -- never let a run get that far.
        try:
            me = client.myself()
        except JiraError as exc:
            raise JiraError(
                f"authentication check failed -- is the PAT valid/unexpired? "
                f"({exc})"
            )
        who = me.get("name") or me.get("key")
        if not who:
            raise JiraError(
                "Jira did not identify us (anonymous session?) -- refusing "
                "to sync; check the PAT"
            )
        print(f"Authenticated : {who}")
    print(f"Project       : {args.project}")
    print(f"Mode          : {mode}")
    print(f"Base label    : {args.label}"
          + ("" if args.label != PROD_LABEL else "   <-- PRODUCTION label"))
    source = args.report or args.groups_file or "hardcoded (iteration 2 sample set)"
    print(f"Groups        : {len(groups)} from {source}")
    if not groups:
        print(
            "NOTE          : no ticket-worthy findings -- every open ticket "
            "will get a close suggestion (if not flagged already)"
        )

    known_components = None
    if not args.dry_run or args.search_in_dry_run:
        known_components = {c.get("name") for c in client.get_components(args.project)}

    tally = {"create": 0, "update": 0, "unchanged": 0}
    problems = []
    for group in groups:
        tally[sync_group(client, group, args, known_components, problems)] += 1

    print("\nDisappeared check (open tickets whose group is no longer reported):")
    if client is None:
        gone_tally = {"flagged": 0}
        print("  skipped: offline dry-run makes no Jira calls "
              "(add --search-in-dry-run to include it)")
    else:
        active_keys = {key_label(args.label, g) for g in groups}
        gone_tally = flag_disappeared(client, args, active_keys, problems)
        if not (gone_tally["flagged"] or gone_tally["already"]):
            print("  every open ticket matches a currently reported group")

    print(
        f"\nSummary: {tally['create']} created, {tally['update']} updated, "
        f"{tally['unchanged']} unchanged, {gone_tally['flagged']} close-suggested"
        + (" (dry-run: nothing was written)" if args.dry_run else "")
    )
    if problems:
        print(f"\n{len(problems)} problem(s) need manual attention:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1
    return 0


# --------------------------------------------------------------------------
# Cleanup (delete throwaway tickets created while developing)
# --------------------------------------------------------------------------
# Values we are willing to auto-fill on a close transition screen, most
# preferred first. MCOL's Close screen runs a workflow validator on
# fixVersions -- ([0-9]+\.[0-9]+\.[0-9]+.*|N/A) -- which rejects an empty
# value even though the REST metadata does not mark the field required,
# hence the explicit "N/A" version. Fields absent from this table are never
# filled in automatically.
AUTO_FILL_PREFERENCES = {
    "resolution": ("Not a Bug", "Won't Fix", "Won't Do", "Incomplete", "Done", "Fixed"),
    "fixVersions": ("N/A",),
}

CLEANUP_COMMENT = (
    "Throwaway ticket from the MCOL-6494 Infer->Jira integration work. "
    "Closing it: the PAT has no delete permission in this project."
)


def status_category(issue):
    return (
        (issue.get("fields", {}).get("status") or {})
        .get("statusCategory", {})
        .get("key", "")
    )


def pick_done_transition(transitions):
    """The first available transition that lands in the 'Done' category."""
    for transition in transitions:
        to = transition.get("to") or {}
        if to.get("statusCategory", {}).get("key") == "done":
            return transition
    return None


def transition_fields(client, project, transition):
    """Fill the transition screen's fields from AUTO_FILL_PREFERENCES.

    Values are chosen from the field's own `allowedValues`, so we adapt to
    the project's workflow instead of hard-coding ids.
    """
    filled = {}
    for name, spec in (transition.get("fields") or {}).items():
        preferences = AUTO_FILL_PREFERENCES.get(name)
        if preferences is None:
            continue  # not ours to guess at -- leave it unset
        allowed = [v.get("name") for v in (spec.get("allowedValues") or []) if v.get("name")]
        if not allowed and name == "fixVersions":
            # Version fields sometimes come back without allowedValues.
            allowed = [v.get("name") for v in client.get_versions(project) if v.get("name")]
        chosen = next((p for p in preferences if p in allowed), None)
        if chosen is None and spec.get("required") and allowed:
            chosen = allowed[0]
        if chosen is None:
            continue
        value = {"name": chosen}
        schema = spec.get("schema") or {}
        filled[name] = [value] if schema.get("type") == "array" else value
    return filled


def describe_fields(fields):
    parts = []
    for name, value in sorted(fields.items()):
        names = [v["name"] for v in value] if isinstance(value, list) else [value["name"]]
        parts.append(f"{name}={'/'.join(names)}")
    return ", ".join(parts)


def close_issue(client, project, key):
    """Close `key` via whatever Done transition the workflow offers."""
    transitions = client.get_transitions(key)
    transition = pick_done_transition(transitions)
    if transition is None:
        names = ", ".join(t.get("name", "?") for t in transitions) or "(none)"
        raise JiraError(f"no transition leads to Done; available: {names}")
    fields = transition_fields(client, project, transition)
    try:
        client.transition_issue(key, transition["id"], fields)
    except JiraError as exc:
        if exc.status == 400:
            offered = ", ".join(sorted((transition.get("fields") or {}))) or "(none)"
            raise JiraError(
                f"{exc}\n    transition '{transition.get('name')}' rejected our "
                f"fields ({describe_fields(fields) or 'none set'}); the screen "
                f"offers: {offered}"
            ) from exc
        raise
    return transition.get("name", transition["id"]), describe_fields(fields)


def cmd_cleanup(args):
    if args.label == PROD_LABEL:
        print(
            f"error: refusing to clean up tickets carrying the production label "
            f"{PROD_LABEL!r}",
            file=sys.stderr,
        )
        return 2
    client = make_client(args)
    jql = f'project = {args.project} AND labels = "{args.label}" ORDER BY created ASC'
    issues = client.search(jql, limit=200)
    print(f"Found {len(issues)} issue(s) matching: {jql}")
    for issue in issues:
        state = "closed" if status_category(issue) == "done" else "open"
        print(f"  - {issue['key']}  [{state}]  {issue['fields'].get('summary', '')}")
    if not issues:
        return 0
    if not args.yes:
        how = "close" if args.close else "delete (falling back to close on HTTP 403)"
        print(f"\nRe-run with --yes to {how} them.")
        return 0

    failures = 0
    for issue in issues:
        key = issue["key"]
        if status_category(issue) == "done":
            print(f"  {key}: already closed, skipping")
            continue
        try:
            if not args.close:
                client.delete_issue(key)
                print(f"  {key}: deleted")
                continue
        except JiraError as exc:
            if exc.status != 403:
                raise
            print(f"  {key}: no delete permission -> closing instead")
        try:
            name, filled = close_issue(client, args.project, key)
            # Comment only after the close succeeded, so a retry after a
            # failed transition does not pile up duplicate comments.
            client.add_comment(key, CLEANUP_COMMENT)
            suffix = f" ({filled})" if filled else ""
            print(f"  {key}: closed via '{name}'{suffix}")
        except JiraError as exc:
            failures += 1
            print(f"  {key}: could not close: {exc}", file=sys.stderr)
    return 1 if failures else 0


# --------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------
def cmd_describe(args):
    """Read-only: what does this project actually accept on create/close?"""
    client = make_client(args)
    print(f"Jira base URL : {client.base_url}")
    print(f"Project       : {args.project} / {args.issue_type}\n")

    meta = client.get_create_meta(args.project, args.issue_type)
    projects = meta.get("projects") or []
    if not projects or not (projects[0].get("issuetypes") or []):
        print(f"No create metadata for {args.project}/{args.issue_type} "
              f"(no create permission, or unknown issue type?)")
        return 1
    fields = projects[0]["issuetypes"][0].get("fields") or {}
    print(f"Create screen fields ({len(fields)}):")
    for field_id, spec in sorted(fields.items()):
        required = "required" if spec.get("required") else "optional"
        allowed = [v.get("name") or v.get("value") for v in (spec.get("allowedValues") or [])]
        line = f"  {field_id:<24} {spec.get('name', '')!r:<28} {required}"
        if allowed:
            shown = ", ".join(str(a) for a in allowed[:12])
            more = f", ... (+{len(allowed) - 12})" if len(allowed) > 12 else ""
            line += f"\n      allowed: {shown}{more}"
        print(line)

    security = fields.get("security")
    print("\nSecurity levels:")
    if security is None:
        print("  (the 'security' field is NOT on the create screen -- tickets "
              "must be restricted after creation)")
    else:
        for value in security.get("allowedValues") or []:
            print(f"  - {value.get('name')}")
        print(f"  configured default: {DEFAULT_SECURITY_LEVEL!r}")

    if args.issue:
        print(f"\nTransitions available on {args.issue}:")
        for transition in client.get_transitions(args.issue):
            to = (transition.get("to") or {}).get("name", "?")
            category = (transition.get("to") or {}).get("statusCategory", {}).get("key")
            print(f"  - {transition.get('name')} (id {transition.get('id')}) "
                  f"-> {to} [{category}]")
            for name, spec in sorted((transition.get("fields") or {}).items()):
                allowed = [v.get("name") or v.get("value")
                           for v in (spec.get("allowedValues") or [])]
                req = "required" if spec.get("required") else "optional"
                extra = f"; allowed: {', '.join(str(a) for a in allowed[:12])}" if allowed else ""
                print(f"      {name} ({req}){extra}")
    return 0


def cmd_parse(args):
    """Iteration 4 verification: show what the report would turn into."""
    groups = report_groups(args)
    print(f"\nGroups ({len(groups)}):")
    for group in groups:
        print(f"  {group['component']} / {group['bug_type']} "
              f"({len(group['locations'])} location(s))")
        for location in group["locations"]:
            print(f"      {location}")
    total = sum(len(g["locations"]) for g in groups)
    print(f"\n{len(groups)} group(s), {total} unique location(s) "
          f"-> would become {len(groups)} ticket(s)")
    return 0


def cmd_check_connection(args):
    """Iteration 0: authenticate and make read-only calls only."""
    client = make_client(args)
    print(f"Jira base URL : {client.base_url}")

    me = client.myself()
    who = me.get("name") or me.get("key") or me.get("displayName", "?")
    print(f"Authenticated : {who} ({me.get('displayName', '')})")

    proj = client.get_project(args.project)
    print(
        f"\nProject       : {proj.get('key')} - {proj.get('name')} "
        f"(id {proj.get('id')}, lead {proj.get('lead', {}).get('name', '?')})"
    )

    components = client.get_components(args.project)
    print(f"\nComponents ({len(components)}):")
    for comp in sorted(components, key=lambda c: c.get("name", "")):
        print(f"  - {comp.get('name')}")

    jql = f"project = {args.project} ORDER BY updated DESC"
    issues = client.search(jql, limit=5)
    print(f'\nJQL check     : {jql}\n'
          f'  total matching: {client.count(jql)}, showing up to 5:')
    for issue in issues:
        fields = issue.get("fields", {})
        status = fields.get("status", {}).get("name", "?")
        print(f"  - {issue.get('key')}  [{status}]  {fields.get('summary', '')}")

    print("\nOK: connectivity, auth, and read permissions look good.")
    return 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def build_parser():
    # Shared options live in a parent parser attached to the main parser AND
    # every subcommand, so `sync --label X` and `--label X sync` both work
    # (argparse otherwise rejects top-level options placed after the
    # subcommand -- exactly how CI calls us). SUPPRESS keeps an unset
    # subcommand option from clobbering a value given before the subcommand;
    # the real defaults are applied in main().
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--url", default=argparse.SUPPRESS,
        help="Jira base URL (default: $JIRA_URL, else https://jira.mariadb.org)",
    )
    common.add_argument(
        "--token-file", default=argparse.SUPPRESS,
        help="File containing the PAT (default: read $JIRA_TOKEN)",
    )
    common.add_argument(
        "--project", default=argparse.SUPPRESS,
        help=f"Jira project key (default: {DEFAULT_PROJECT})",
    )
    common.add_argument(
        "--label", default=argparse.SUPPRESS,
        help=f"Base label identifying our tickets (default: {DEFAULT_LABEL}; "
             f"{PROD_LABEL} is the production one)",
    )

    parser = argparse.ArgumentParser(
        description="Create/update Jira tickets from ColumnStore Infer findings.",
        parents=[common],
    )

    sub = parser.add_subparsers(dest="command", required=True)

    p_check = sub.add_parser(
        "check-connection",
        help="Iteration 0: read-only auth/permissions check (no writes).",
        parents=[common],
    )
    p_check.set_defaults(func=cmd_check_connection)

    p_sync = sub.add_parser(
        "sync",
        help="Create or update the tickets for the current finding groups.",
        parents=[common],
    )
    p_sync.add_argument(
        "--dry-run", action="store_true",
        help="Print the intended actions and write nothing.",
    )
    p_sync.add_argument(
        "--search-in-dry-run", action="store_true",
        help="In dry-run, still query Jira so existing tickets are detected "
             "(read-only; without it a dry-run makes no Jira calls at all).",
    )
    p_sync.add_argument(
        "--report",
        help="Infer report.json to parse into finding groups (the real "
             "input; mutually exclusive with --groups-file).",
    )
    p_sync.add_argument(
        "--include-suppressed", action="store_true",
        help="With --report: also ticket @infer-ignore-suppressed findings. "
             "TEST ONLY -- production never tickets suppressed findings.",
    )
    p_sync.add_argument(
        "--groups-file",
        help="JSON file with the finding groups (a list of objects with "
             "component / bug_type / locations); a hand-made test input.",
    )
    p_sync.add_argument(
        "--assignee", default=DEFAULT_ASSIGNEE,
        help=f"Jira username to assign tickets to (default: {DEFAULT_ASSIGNEE})",
    )
    p_sync.add_argument(
        "--issue-type", default=DEFAULT_ISSUE_TYPE,
        help=f"Issue type for new tickets (default: {DEFAULT_ISSUE_TYPE})",
    )
    p_sync.add_argument(
        "--branch",
        help="Branch the report was scanned from, recorded in descriptions "
             "(CI passes $DRONE_BRANCH; constant per branch, so descriptions "
             "stay stable across runs).",
    )
    p_sync.add_argument(
        "--security-level", default=DEFAULT_SECURITY_LEVEL,
        help=f"Issue security level for new tickets "
             f"(default: {DEFAULT_SECURITY_LEVEL!r})",
    )
    p_sync.add_argument(
        "--no-security-level", dest="security_level", action="store_const", const=None,
        help="Create PUBLIC tickets with no security level. Findings are "
             "potential vulnerabilities -- only for a project without an "
             "issue security scheme.",
    )
    p_sync.set_defaults(func=cmd_sync)

    p_desc = sub.add_parser(
        "describe",
        help="Print what the project's create screen and workflow accept "
             "(security levels, versions, transitions). Read-only.",
        parents=[common],
    )
    p_desc.add_argument(
        "--issue-type", default=DEFAULT_ISSUE_TYPE,
        help=f"Issue type to inspect (default: {DEFAULT_ISSUE_TYPE})",
    )
    p_desc.add_argument(
        "--issue", help="Also list the transitions available on this issue key.",
    )
    p_desc.set_defaults(func=cmd_describe)

    p_parse = sub.add_parser(
        "parse",
        help="Parse a report.json into groups and print them (no Jira, "
             "no token needed) -- for verifying parsing + component mapping.",
        parents=[common],
    )
    p_parse.add_argument("--report", required=True, help="Infer report.json path.")
    p_parse.add_argument(
        "--include-suppressed", action="store_true",
        help="Also include @infer-ignore-suppressed findings (test only).",
    )
    p_parse.set_defaults(func=cmd_parse)

    p_clean = sub.add_parser(
        "cleanup",
        help="Delete (or close) every ticket carrying --label; test labels only.",
        parents=[common],
    )
    p_clean.add_argument(
        "--yes", action="store_true", help="Actually act (default: list only)."
    )
    p_clean.add_argument(
        "--close", action="store_true",
        help="Close the tickets instead of deleting them. Deleting is tried "
             "first by default and already falls back to closing on HTTP 403.",
    )
    p_clean.set_defaults(func=cmd_cleanup)

    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    # The shared options use SUPPRESS (see build_parser), so an attribute is
    # absent unless the user passed it -- apply the real defaults here.
    args.url = getattr(args, "url", None)
    args.token_file = getattr(args, "token_file", None)
    args.project = getattr(args, "project", DEFAULT_PROJECT)
    args.label = getattr(args, "label", DEFAULT_LABEL)
    try:
        return args.func(args)
    except JiraError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
