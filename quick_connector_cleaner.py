#!/usr/bin/env python3
"""Consolidate duplicated AWS Quick workspace connectors onto one canonical connector.

When an Enterprise-edition account has no centrally managed connector, every user
creates their own Gmail / Google Calendar / Outlook / ... action connector. Only one
connector per service works reliably, and sharing an agent or flow then also requires
sharing the creator's private connector.

Once the IT team has created the centralized ("canonical") connector, this script:

1. Finds the duplicated workspace action connectors (Google Workspace and Microsoft 365),
   grouped by connector type.
2. Finds every agent, flow and space that uses each duplicate, and who owns it.
3. Shares the canonical connector with the duplicate's users (optional).
4. Re-points those agents, flows and spaces to the canonical connector.
5. Verifies nothing still references the duplicate, then deletes it.

The default run is a dry run that only reads and writes a plan. Changes need --apply.

Example:
    python quick_connector_cleaner.py --profile my-admin --region us-west-2
    python quick_connector_cleaner.py --region us-west-2 \\
        --canonical 0f3c...-gmail-id --canonical 9a1b...-calendar-id --apply
"""
from __future__ import annotations

import argparse
import copy
import csv
import datetime as dt
import json
import logging
import re
import shutil
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

LOG = logging.getLogger("quick-connector-cleaner")

# Connector types in scope by default: Google Workspace (Gmail, Google Calendar, Google Drive, ...)
# and Microsoft 365 (MICROSOFT_OUTLOOK, MICROSOFT_ONEDRIVE, MICROSOFT_SHAREPOINT, MICROSOFT_TEAMS).
# Google type names are matched loosely because they are newer than some SDK enums.
DEFAULT_TYPE_PATTERN = r"GOOGLE|GMAIL|^MICROSOFT_"

# Hosts that identify a hand-built GENERIC_HTTP connector pointing at a workspace API.
# Those are reported for manual review only: their action IDs differ from the managed type.
WORKSPACE_URL_MARKERS = (
    "googleapis.com",
    "accounts.google.com",
    "graph.microsoft.com",
    "login.microsoftonline.com",
    "outlook.office",
    "sharepoint.com",
)

FLOW_STATES = ("PUBLISHED", "DRAFT")  # the versions UpdateFlow writes (one definition for both)
PENDING_APPROVAL = "PENDING_APPROVAL"
ALL_FLOW_STATES = FLOW_STATES + (PENDING_APPROVAL,)  # read during the scan so nothing goes unseen
# ResourcePermission: action connectors accept Quick user and group principals only.
GRANTABLE_PRINCIPAL = re.compile(r"^arn:aws[\w-]*:quicksight:[\w-]*:\d{12}:(user|group)/")
MAX_ACTIONS_PER_PERMISSION = 20  # ResourcePermission.Actions: 1-20 items
# Quick user ARN: arn:aws:quicksight:<region>:<account>:user/<namespace>/<user name, may contain "/">
USER_ARN = re.compile(r"^arn:aws[\w-]*:quicksight:[\w-]*:\d{12}:user/(?P<ns>[^/]+)/(?P<name>.+)$")
ACCESS_ISSUES_FILE = "access_issues.csv"
HEALTHY_STATUSES = {"CREATION_SUCCESSFUL", "UPDATE_SUCCESSFUL", None}
MIN_SAFE_ID_LENGTH = 8


# --------------------------------------------------------------------------------------
# Data model
# --------------------------------------------------------------------------------------
@dataclass
class Connector:
    id: str
    arn: str
    name: str
    type: str
    status: str | None
    created: Any
    updated: Any
    description: str | None = None
    auth_type: str | None = None
    endpoints: list[str] = field(default_factory=list)
    enabled_actions: list[str] = field(default_factory=list)
    permissions: list[dict] = field(default_factory=list)

    def principals(self) -> list[str]:
        return [p["Principal"] for p in self.permissions]

    def owners(self) -> list[str]:
        return owners_of(self.permissions)

    def matches(self, value: str) -> bool:
        return value == self.id or value == self.arn or value.endswith("/" + self.id)

    def summary(self) -> dict:
        return {
            "id": self.id,
            "arn": self.arn,
            "name": self.name,
            "type": self.type,
            "status": self.status,
            "created": str(self.created),
            "auth_type": self.auth_type,
            "owners": self.owners(),
            "principals": self.principals(),
            "enabled_actions_count": len(self.enabled_actions),
        }


@dataclass
class Reference:
    kind: str  # agent | flow | space
    resource_id: str
    name: str
    connector_id: str
    owners: list[str]
    detail: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "kind": self.kind,
            "id": self.resource_id,
            "name": self.name,
            "connector_id": self.connector_id,
            "owners": self.owners,
            **self.detail,
        }


@dataclass
class Group:
    type: str
    connectors: list[Connector]
    canonical: Connector | None = None
    skip_reason: str | None = None

    @property
    def duplicates(self) -> list[Connector]:
        if not self.canonical:
            return []
        return [c for c in self.connectors if c.id != self.canonical.id]

    @property
    def candidates(self) -> list[Connector]:
        """Connectors that are (or may become) duplicates: everything except the canonical one."""
        if self.canonical:
            return self.duplicates
        return self.connectors if len(self.connectors) > 1 else []


# --------------------------------------------------------------------------------------
# AWS API wrapper
# --------------------------------------------------------------------------------------
def error_code(exc: ClientError) -> str:
    return exc.response.get("Error", {}).get("Code", "")


class QuickApi:
    """Thin wrapper over the QuickSight client: pagination plus account id injection."""

    def __init__(self, client, account_id: str):
        self.c = client
        self.account = account_id

    def _pages(self, method: Callable, key: str, **kwargs) -> Iterator[dict]:
        """Yield every item of a paginated List call, following NextToken until the last page.

        Pages can be empty and still carry a NextToken, so only a missing token ends the loop.
        A token seen twice means the service is not advancing: stop with an error instead of
        looping forever, because acting on a partial list (for example a flow missed during the
        scan) could delete a connector that is still in use.
        """
        token, seen, page = None, set(), 0
        while True:
            params = dict(kwargs, AwsAccountId=self.account)
            if token:
                params["NextToken"] = token
            resp = method(**params)
            page += 1
            yield from resp.get(key) or []
            token = resp.get("NextToken")
            if not token:
                LOG.debug("%s: %d page(s)", getattr(method, "__name__", method), page)
                return
            if token in seen:
                raise RuntimeError(f"{getattr(method, '__name__', method)} returned the same NextToken twice "
                                   f"(page {page}); stopping so the run doesn't act on an incomplete list")
            seen.add(token)

    # Connectors
    def list_connectors(self) -> list[dict]:
        return list(self._pages(self.c.list_action_connectors, "ActionConnectorSummaries", MaxResults=100))

    def describe_connector(self, cid: str) -> dict:
        return self.c.describe_action_connector(AwsAccountId=self.account, ActionConnectorId=cid)["ActionConnector"]

    def connector_permissions(self, cid: str) -> list[dict]:
        resp = self.c.describe_action_connector_permissions(AwsAccountId=self.account, ActionConnectorId=cid)
        return resp.get("Permissions") or []

    def grant_connector_permissions(self, cid: str, grants: list[dict]) -> None:
        for i in range(0, len(grants), 64):  # API limit: 64 permissions per call
            self.c.update_action_connector_permissions(
                AwsAccountId=self.account, ActionConnectorId=cid, GrantPermissions=grants[i : i + 64]
            )

    def list_user_groups(self, user_name: str, namespace: str) -> list[dict]:
        return list(self._pages(self.c.list_user_groups, "GroupList",
                                UserName=user_name, Namespace=namespace, MaxResults=100))

    def delete_connector(self, cid: str) -> None:
        self.c.delete_action_connector(AwsAccountId=self.account, ActionConnectorId=cid)

    # Agents
    def list_agents(self) -> list[dict]:
        return list(self._pages(self.c.list_agents, "AgentSummaries", MaxResults=100))

    def describe_agent(self, agent_id: str) -> dict:
        return self.c.describe_agent(AwsAccountId=self.account, AgentId=agent_id)["Agent"]

    def agent_permissions(self, agent_id: str) -> list[dict]:
        return self.c.describe_agent_permissions(AwsAccountId=self.account, AgentId=agent_id).get("Permissions") or []

    def update_agent_permissions(self, agent_id: str, grant: list[dict], revoke: list[dict]) -> None:
        kwargs: dict[str, Any] = {"AwsAccountId": self.account, "AgentId": agent_id}
        if grant:
            kwargs["GrantPermissions"] = grant
        if revoke:
            kwargs["RevokePermissions"] = revoke
        self.c.update_agent_permissions(**kwargs)

    def update_agent(self, **kwargs) -> dict:
        return self.c.update_agent(AwsAccountId=self.account, **kwargs)

    # Flows
    def list_flows(self) -> list[dict]:
        return list(self._pages(self.c.list_flows, "FlowSummaryList", MaxResults=100))

    def describe_flow(self, flow_id: str, state: str) -> dict | None:
        try:
            return self.c.describe_flow(AwsAccountId=self.account, FlowId=flow_id, PublishState=state)["Flow"]
        except ClientError as exc:
            # Only "this version does not exist" may be treated as missing. Any other error must surface:
            # treating it as "no draft" would let UpdateFlow overwrite a draft we never read.
            # Observed: a missing version returns ValidationException "appVersion is not found for the stage ...".
            if error_code(exc) == "ResourceNotFoundException" or (
                error_code(exc) == "ValidationException" and "appVersion is not found" in str(exc)
            ):
                return None
            raise

    def flow_permissions(self, flow_id: str) -> list[dict]:
        return self.c.get_flow_permissions(AwsAccountId=self.account, FlowId=flow_id).get("Permissions") or []

    def create_flow(self, name: str, description: str, definition: Any, permissions: list[dict]) -> str:
        resp = self.c.create_flow(AwsAccountId=self.account, Name=name, Description=description,
                                  FlowDefinition=definition, Permissions=permissions,
                                  ClientToken=str(uuid.uuid4()))
        return resp["FlowId"]

    def delete_flow(self, flow_id: str) -> None:
        self.c.delete_flow(AwsAccountId=self.account, FlowId=flow_id)

    def describe_user(self, user_name: str, namespace: str) -> dict:
        return self.c.describe_user(AwsAccountId=self.account, UserName=user_name, Namespace=namespace)["User"]

    def update_flow_permissions(self, flow_id: str, grant: list[dict], revoke: list[dict]) -> None:
        kwargs: dict[str, Any] = {"AwsAccountId": self.account, "FlowId": flow_id}
        if grant:
            kwargs["GrantPermissions"] = grant
        if revoke:
            kwargs["RevokePermissions"] = revoke
        self.c.update_flow_permissions(**kwargs)

    def update_flow(self, flow_id: str, definition: Any) -> None:
        self.c.update_flow(
            AwsAccountId=self.account, FlowId=flow_id, FlowDefinition=definition, ClientToken=str(uuid.uuid4())
        )

    # Spaces
    def list_spaces(self) -> list[dict]:
        return list(self._pages(self.c.list_spaces, "SpaceSummaries", MaxResults=500))

    def space_resources(self, space_id: str) -> list[dict]:
        return self.c.list_space_resources(AwsAccountId=self.account, SpaceId=space_id).get("SpaceResources") or []

    def update_space_resources(self, space_id: str, add: list[str], remove: list[str]) -> dict:
        def ops(arns):
            return [{"ResourceType": "ACTION_CONNECTOR", "ResourceDetails": {"resourceArn": a}} for a in arns]

        kwargs = {"AwsAccountId": self.account, "SpaceId": space_id}
        if add:
            kwargs["AddResources"] = ops(add)
        if remove:
            kwargs["RemoveResources"] = ops(remove)
        return self.c.update_space_resources(**kwargs)


def owners_of(permissions: list[dict]) -> list[str]:
    """Principals with management rights (delete or permission updates) on a resource."""
    owners = [
        p["Principal"]
        for p in permissions
        if any(("Delete" in a) or ("Permissions" in a and "Update" in a) for a in p.get("Actions", []))
    ]
    return owners or [p["Principal"] for p in permissions]


# --------------------------------------------------------------------------------------
# Reference search and rewrite inside flow definitions
# --------------------------------------------------------------------------------------
def _id_pattern(connector_id: str) -> re.Pattern:
    return re.compile(r"(?<![\w-])" + re.escape(connector_id) + r"(?![\w-])")


def _strings(obj: Any) -> Iterator[str]:
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            yield str(k)
            yield from _strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _strings(v)


def references_connector(obj: Any, conn: Connector) -> int:
    """Count string values (and keys) in obj that reference conn by ARN or ID."""
    pattern = _id_pattern(conn.id)
    safe_id = len(conn.id) >= MIN_SAFE_ID_LENGTH
    hits = 0
    for s in _strings(obj):
        if conn.arn in s or s == conn.id or (safe_id and pattern.search(s)):
            hits += 1
    return hits


# A flow step that calls a connector looks like this (DescribeFlow, observed 2026-11):
#   {"StepId": "...", "StepType": "CONNECTOR_ACTIONS", "StepTitle": "...",
#    "StepConfig": {"ConnectorActionsConfig": {"prompt": "...", "actionConnectorId": "<id>",
#                                              "actionConnectorType": "GMAIL", "actionId": "ListEmails"}}}
# The three connector fields must stay consistent: actionId must be one of the target connector's
# EnabledActions, and actionConnectorType must be the target connector's Type.
CONNECTOR_STEP_CONFIG = "ConnectorActionsConfig"


@dataclass
class ConnectorStep:
    step_id: str
    title: str
    config: dict  # the ConnectorActionsConfig dict, mutable in place

    def signature(self) -> tuple:
        c = self.config
        return (self.step_id, c.get("actionConnectorId"), c.get("actionConnectorType"), c.get("actionId"))


def connector_steps(definition: Any) -> list[ConnectorStep]:
    """Every ConnectorActionsConfig in a flow definition, with its enclosing step (nested steps included)."""
    found: list[ConnectorStep] = []

    def walk(o: Any, step_id: str, title: str) -> None:
        if isinstance(o, dict):
            step_id = o.get("StepId", step_id)
            title = o.get("StepTitle", title)
            cfg = o.get(CONNECTOR_STEP_CONFIG)
            if isinstance(cfg, dict):
                found.append(ConnectorStep(step_id, title, cfg))
            for v in o.values():
                walk(v, step_id, title)
        elif isinstance(o, list):
            for v in o:
                walk(v, step_id, title)

    walk(definition, "?", "?")
    return found


def rewrite_flow(definition: Any, old: Connector, new: Connector) -> tuple[Any, list[dict], list[str]]:
    """Point every connector step that uses old at new, keeping the step's action valid.

    Returns (new definition, changed steps, problems). Any problem means the flow must not be updated.
    """
    new_def = copy.deepcopy(definition)
    changes: list[dict] = []
    problems: list[str] = []
    enabled = set(new.enabled_actions)
    for st in connector_steps(new_def):
        cfg = st.config
        current = str(cfg.get("actionConnectorId") or "")
        if not current or not old.matches(current):
            continue
        label = f"step {st.title!r} ({st.step_id})"
        action = cfg.get("actionId")
        if cfg.get("actionConnectorType") not in (None, old.type):
            problems.append(f"{label}: actionConnectorType {cfg.get('actionConnectorType')} != duplicate type {old.type}")
        if new.type != old.type:
            problems.append(f"{label}: canonical type {new.type} != duplicate type {old.type}")
        if not action:
            problems.append(f"{label}: no actionId set")
        elif not enabled:
            problems.append(f"{label}: canonical connector reports no enabled actions, cannot validate {action}")
        elif action not in enabled:
            problems.append(f"{label}: action {action} is not enabled on the canonical connector")
        cfg["actionConnectorId"] = new.arn if current.startswith("arn:") else new.id
        cfg["actionConnectorType"] = new.type
        changes.append({"step_id": st.step_id, "title": st.title, "actionId": action})
    # No change is not a problem by itself: one version of a flow may not use the duplicate.
    # Callers decide (see FlowPlan). Anything still pointing at the duplicate lives outside the
    # known step format: don't guess.
    leftover = references_connector(new_def, old)
    if leftover:
        problems.append(f"{leftover} reference(s) to the duplicate outside ConnectorActionsConfig; not rewritten")
    return new_def, changes, problems


def permission_map(permissions: list[dict]) -> dict[str, frozenset]:
    """principal -> set of actions (the API can list the same action twice)."""
    out: dict[str, set] = {}
    for p in permissions or []:
        out.setdefault(p["Principal"], set()).update(p.get("Actions") or [])
    return {k: frozenset(v) for k, v in out.items()}


def permission_diff(before: list[dict], after: list[dict]) -> tuple[list[dict], list[dict]]:
    """(grants that put back what was removed, revokes that take away what was added)."""
    b, a = permission_map(before), permission_map(after)
    grant = [{"Principal": p, "Actions": sorted(acts - a.get(p, frozenset()))}
             for p, acts in b.items() if acts - a.get(p, frozenset())]
    revoke = [{"Principal": p, "Actions": sorted(acts - b.get(p, frozenset()))}
              for p, acts in a.items() if acts - b.get(p, frozenset())]
    return grant, revoke


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, default=str)


def _steps(definition: Any) -> list[dict]:
    steps = definition.get("Steps") if isinstance(definition, dict) else None
    return [s for s in steps if isinstance(s, dict)] if isinstance(steps, list) else []


def step_diff(published: Any, draft: Any) -> list[str]:
    """How the draft differs from the published version, step by step (empty list = identical).

    Compared: the steps in order (StepId, StepType, StepTitle, the whole StepConfig) and every other
    field of the FlowDefinition. Ignored: JSON key order; metadata outside the definition (PublishState,
    LastUpdatedTime, LastUpdatedBy, StepAliases).
    """
    if canonical_json(published) == canonical_json(draft):
        return []
    diffs: list[str] = []
    p_steps, d_steps = _steps(published), _steps(draft)
    p_by_id = {s.get("StepId"): s for s in p_steps}
    d_by_id = {s.get("StepId"): s for s in d_steps}

    def label(s: dict) -> str:
        return f"{s.get('StepTitle')!r} ({s.get('StepId')})"

    added = [label(s) for s in d_steps if s.get("StepId") not in p_by_id]
    removed = [label(s) for s in p_steps if s.get("StepId") not in d_by_id]
    changed = []
    for s in d_steps:
        old = p_by_id.get(s.get("StepId"))
        if old is None or canonical_json(old) == canonical_json(s):
            continue
        fields = sorted(k for k in set(old) | set(s) if canonical_json(old.get(k)) != canonical_json(s.get(k)))
        if "StepConfig" in fields:  # name the config fields that changed (prompt, actionId, ...)
            oc = next(iter((old.get("StepConfig") or {}).values()), {}) or {}
            nc = next(iter((s.get("StepConfig") or {}).values()), {}) or {}
            if isinstance(oc, dict) and isinstance(nc, dict):
                inner = sorted(k for k in set(oc) | set(nc) if canonical_json(oc.get(k)) != canonical_json(nc.get(k)))
                fields = [f for f in fields if f != "StepConfig"] + (inner or ["StepConfig"])
        changed.append(f"{label(s)} [{', '.join(fields)}]")
    common_p = [s.get("StepId") for s in p_steps if s.get("StepId") in d_by_id]
    common_d = [s.get("StepId") for s in d_steps if s.get("StepId") in p_by_id]
    if added:
        diffs.append("added: " + "; ".join(added))
    if removed:
        diffs.append("removed: " + "; ".join(removed))
    if changed:
        diffs.append("changed: " + "; ".join(changed))
    if common_p != common_d:
        diffs.append("steps reordered")
    others = sorted(k for k in set(published or {}) | set(draft or {})
                    if k != "Steps" and canonical_json((published or {}).get(k)) != canonical_json((draft or {}).get(k)))
    if others:
        diffs.append("other definition fields changed: " + ", ".join(others))
    return diffs or ["definitions differ"]


@dataclass
class FlowPlan:
    """How a flow will be migrated.

    mode "single": one definition is rewritten and written to both versions with UpdateFlow.
    mode "copy-draft": the draft has unpublished edits. The rewritten draft goes into a new flow shared with
        the owners only; the rewritten published version is then written to the original flow.
    mode None: the flow is left unchanged (reason says why).
    """
    mode: str | None
    reason: str = ""
    source: Any = None          # single: the definition to rewrite
    published: Any = None       # copy-draft
    draft: Any = None           # copy-draft
    draft_changes: list[str] = field(default_factory=list)  # step_diff(published, draft)


def plan_flow(states: dict[str, Any], divergence: str) -> FlowPlan:
    """Decide how to handle the flow's versions. UpdateFlow writes one definition to both draft and published,
    and the API has no draft-only update, so different versions are kept apart with a copy."""
    if PENDING_APPROVAL in states:
        return FlowPlan(None, "a version is pending approval, and the API can't update it separately; "
                              "approve or reject it in Quick, then run again")
    published, draft = states.get("PUBLISHED"), states.get("DRAFT")
    diffs = step_diff(published, draft) if published is not None and draft is not None else []
    if not diffs:
        return FlowPlan("single", source=published if published is not None else draft)
    summary = "; ".join(diffs)
    if divergence == "copy-draft":
        return FlowPlan("copy-draft", published=published, draft=draft, draft_changes=diffs)
    if divergence == "use-draft":
        return FlowPlan("single", source=draft, draft_changes=diffs)
    if divergence == "use-published":
        return FlowPlan("single", source=published, draft_changes=diffs)
    return FlowPlan(None, f"draft has unpublished edits ({summary}); left unchanged (--flow-divergence skip)",
                    draft_changes=diffs)


def plan_rewrites(plan: FlowPlan, dup: Connector, canon: Connector) -> tuple[dict[str, Any], list[str]]:
    """Rewrite the definition(s) of a plan. Returns ({"source"|"published"|"draft": new definition}, problems)."""
    if plan.mode == "single":
        new_def, changes, problems = rewrite_flow(plan.source, dup, canon)
        if not changes:
            problems.append("no ConnectorActionsConfig step uses the duplicate")
        return {"source": new_def}, problems
    new_pub, pub_changes, pub_problems = rewrite_flow(plan.published, dup, canon)
    new_draft, draft_changes, draft_problems = rewrite_flow(plan.draft, dup, canon)
    problems = [f"published: {p}" for p in pub_problems] + [f"draft: {p}" for p in draft_problems]
    if not pub_changes and not draft_changes:
        problems.append("no ConnectorActionsConfig step uses the duplicate in either version")
    return {"published": new_pub, "draft": new_draft}, problems


OWNER_FLOW_ACTIONS = ("quicksight:DeleteFlow", "quicksight:UpdateFlowPermissions")
FLOW_TEXT_FORBIDDEN = re.compile(r'[{}"\\<>]')  # CreateFlow Name/Description pattern: (?!\s+$)[^{}"\\<>]*
DRAFT_COPY_SUFFIX = " (draft copy - copie brouillon)"


def owner_permissions(permissions: list[dict]) -> list[dict]:
    """The owner entries of a flow (users or groups that can delete it or change its sharing), as they are."""
    merged = permission_map(permissions)
    return [{"Principal": p, "Actions": sorted(acts)} for p, acts in merged.items()
            if GRANTABLE_PRINCIPAL.match(p) and any(a in acts for a in OWNER_FLOW_ACTIONS)]


def _flow_text(text: str, limit: int) -> str:
    return FLOW_TEXT_FORBIDDEN.sub("", text).strip()[:limit]


def draft_copy_name(original: str) -> str:
    base = _flow_text(original, 128 - len(DRAFT_COPY_SUFFIX)) or "Flow"
    return base + DRAFT_COPY_SUFFIX


def draft_copy_description(original: str, flow_id: str, dup: Connector, canon: Connector) -> str:
    day = dt.date.today().isoformat()
    name, old, new = _flow_text(original, 128), _flow_text(dup.name, 100), _flow_text(canon.name, 100)
    text = (f"FR : Copie du brouillon non publié du flow « {name} » ({flow_id}), créée le {day} par votre équipe "
            f"IT lors du nettoyage des connecteurs. Elle utilise le connecteur partagé « {new} » au lieu de "
            f"« {old} ». Vous seul la voyez. Le flow original conserve sa version publiée, désormais sur « {new} ». "
            f"Poursuivez vos modifications ici ou reportez-les dans l'original, puis supprimez cette copie. "
            f"EN: Copy of the unpublished draft of flow '{name}' ({flow_id}), created on {day} by your IT team "
            f"during the connector cleanup. It uses the shared connector '{new}' instead of '{old}'. Only you can "
            f"see it. The original flow keeps its published version, now on '{new}'. Continue your edits here or "
            f"copy them into the original, then delete this copy.")
    return _flow_text(text, 1024)


def flow_preflight(usage: "Usage", flow_id: str, dup: Connector, canon: Connector, divergence: str) -> list[str]:
    """Problems that would stop this flow from being migrated, computed from the scan (dry run)."""
    plan = plan_flow(usage.flow_defs.get(flow_id, {}), divergence)
    if plan.mode is None:
        return [plan.reason]
    return plan_rewrites(plan, dup, canon)[1]


# --------------------------------------------------------------------------------------
# Discovery
# --------------------------------------------------------------------------------------
def _endpoints(auth_config: dict | None) -> list[str]:
    return sorted({s for s in _strings(auth_config or {}) if s.startswith("http")})


def load_connector(api: QuickApi, summary: dict) -> Connector:
    detail = api.describe_connector(summary["ActionConnectorId"])
    auth = detail.get("AuthenticationConfig") or {}
    return Connector(
        id=summary["ActionConnectorId"],
        arn=summary["Arn"],
        name=summary["Name"],
        type=summary["Type"],
        status=summary.get("Status"),
        created=summary.get("CreatedTime"),
        updated=summary.get("LastUpdatedTime"),
        description=detail.get("Description"),
        auth_type=auth.get("AuthenticationType"),
        endpoints=_endpoints(auth),
        enabled_actions=detail.get("EnabledActions") or [],
        permissions=api.connector_permissions(summary["ActionConnectorId"]),
    )


def discover_connectors(api: QuickApi, type_pattern: re.Pattern) -> tuple[list[Connector], list[Connector]]:
    """Return (in-scope workspace connectors, GENERIC_HTTP connectors targeting workspace APIs)."""
    in_scope, generic = [], []
    summaries = [s for s in api.list_connectors() if s.get("Status") != "DELETED"]
    LOG.info("Found %d action connectors in account %s", len(summaries), api.account)
    for s in summaries:
        if type_pattern.search(s["Type"] or ""):
            in_scope.append(load_connector(api, s))
        elif s["Type"] == "GENERIC_HTTP":
            conn = load_connector(api, s)
            if any(m in url for url in conn.endpoints for m in WORKSPACE_URL_MARKERS):
                generic.append(conn)
    LOG.info("%d workspace connectors in scope, %d custom HTTP connectors target workspace APIs",
             len(in_scope), len(generic))
    return in_scope, generic


@dataclass
class Usage:
    refs: dict[str, list[Reference]] = field(default_factory=dict)  # connector id -> references
    flow_defs: dict[str, dict[str, Any]] = field(default_factory=dict)  # flow id -> {state: definition}
    flow_names: dict[str, str] = field(default_factory=dict)
    agents: dict[str, dict] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def add(self, ref: Reference) -> None:
        self.refs.setdefault(ref.connector_id, []).append(ref)

    def of(self, conn: Connector) -> list[Reference]:
        return self.refs.get(conn.id, [])


def scan_usage(api: QuickApi, connectors: list[Connector]) -> Usage:
    usage = Usage()
    if not connectors:
        return usage

    # Agents: the agent definition lists its action connector ARNs directly.
    agents = api.list_agents()
    LOG.info("Scanning %d agents", len(agents))
    for summary in agents:
        aid = summary["AgentId"]
        try:
            agent = api.describe_agent(aid)
        except ClientError as exc:
            usage.errors.append(f"agent {aid}: {exc}")
            continue
        usage.agents[aid] = agent
        linked = agent.get("ActionConnectors") or []
        users = None
        for conn in connectors:
            if any(conn.matches(a) for a in linked):
                if users is None:
                    users = _safe_owners(api.agent_permissions, aid, usage) or [agent.get("Creator", "?")]
                usage.add(Reference("agent", aid, agent["Name"], conn.id, users,
                                    {"creator": agent.get("Creator"), "lifecycle": agent.get("AgentLifecycle")}))

    # Flows: connectors are referenced from inside the step definitions (draft and published).
    flows = api.list_flows()
    LOG.info("Scanning %d flows", len(flows))
    for summary in flows:
        fid = summary["FlowId"]
        usage.flow_names[fid] = summary["Name"]
        states: dict[str, Any] = {}
        for state in ALL_FLOW_STATES:
            try:
                flow = api.describe_flow(fid, state)
            except ClientError as exc:
                usage.errors.append(f"flow {fid} ({state}): {exc}")
                continue
            if flow is not None:
                states[state] = flow.get("FlowDefinition")
        usage.flow_defs[fid] = states
        users = None
        for conn in connectors:
            hits = {st: references_connector(d, conn) for st, d in states.items()}
            if any(hits.values()):
                if users is None:
                    users = _safe_owners(api.flow_permissions, fid, usage) or [summary.get("CreatedBy", "?")]
                usage.add(Reference("flow", fid, summary["Name"], conn.id, users,
                                    {"created_by": summary.get("CreatedBy"),
                                     "publish_state": summary.get("PublishState"),
                                     "hits_by_state": {k: v for k, v in hits.items() if v}}))

    # Spaces: action connectors can be attached to a space as a resource.
    spaces = api.list_spaces()
    LOG.info("Scanning %d spaces", len(spaces))
    for space in spaces:
        sid = space["spaceId"]
        try:
            resources = api.space_resources(sid)
        except ClientError as exc:
            usage.errors.append(f"space {sid}: {exc}")
            continue
        arns = [r.get("ResourceDetails", {}).get("resourceArn", "")
                for r in resources if r.get("ResourceType") == "ACTION_CONNECTOR"]
        for conn in connectors:
            if any(conn.matches(a) for a in arns if a):
                usage.add(Reference("space", sid, space.get("name") or sid, conn.id,
                                    [space.get("createdByArn") or space.get("createdBy") or "?"]))
    return usage


def _safe_owners(fn: Callable[[str], list[dict]], rid: str, usage: Usage) -> list[str]:
    try:
        return owners_of(fn(rid))
    except ClientError as exc:
        usage.errors.append(f"permissions of {rid}: {exc}")
        return []


# --------------------------------------------------------------------------------------
# Planning: grouping and canonical selection
# --------------------------------------------------------------------------------------
def build_groups(connectors: list[Connector]) -> list[Group]:
    by_type: dict[str, list[Connector]] = {}
    for c in connectors:
        by_type.setdefault(c.type, []).append(c)
    return [Group(t, sorted(cs, key=lambda c: str(c.created))) for t, cs in sorted(by_type.items())]


def parse_connector_ref(value: str) -> str:
    """Accept a connector ID or ARN and return the ID."""
    return value.rsplit("/", 1)[-1].strip()


def select_canonicals(groups: list[Group], usage: Usage, wanted: list[str], interactive: bool) -> None:
    wanted_ids = {parse_connector_ref(w) for w in wanted}
    known = {c.id for g in groups for c in g.connectors}
    for unknown in sorted(wanted_ids - known):
        LOG.error("--canonical %s is not an in-scope workspace connector in this account/region", unknown)

    for g in groups:
        chosen = [c for c in g.connectors if c.id in wanted_ids]
        if len(chosen) > 1:
            g.skip_reason = "more than one --canonical given for this type"
            continue
        if chosen:
            g.canonical = chosen[0]
        elif len(g.connectors) < 2:
            g.skip_reason = "only one connector of this type, nothing to consolidate"
            continue
        elif interactive:
            g.canonical = prompt_canonical(g, usage)
            if not g.canonical:
                g.skip_reason = "skipped at prompt"
                continue
        else:
            g.skip_reason = "no canonical connector chosen (use --canonical or run interactively)"
            continue
        if g.canonical.status not in HEALTHY_STATUSES:
            g.skip_reason = f"canonical connector status is {g.canonical.status}"
            g.canonical = None
        elif not g.duplicates:
            g.skip_reason = "no duplicates besides the canonical connector"


def prompt_canonical(g: Group, usage: Usage) -> Connector | None:
    print(f"\n{len(g.connectors)} connectors of type {g.type}. Which one is the legitimate (IT-managed) one?")
    for i, c in enumerate(g.connectors, 1):
        refs = usage.of(c)
        kinds = {k: sum(1 for r in refs if r.kind == k) for k in ("agent", "flow", "space")}
        print(f"  [{i}] {c.name!r}  id={c.id}  status={c.status}  created={c.created}")
        print(f"      owners={', '.join(c.owners()) or '-'}  shared with {len(c.principals())} principal(s)  "
              f"agents={kinds['agent']} flows={kinds['flow']} spaces={kinds['space']}")
    while True:
        answer = input(f"Choose 1-{len(g.connectors)}, or 's' to skip this type: ").strip().lower()
        if answer in ("s", "skip", ""):
            return None
        if answer.isdigit() and 1 <= int(answer) <= len(g.connectors):
            return g.connectors[int(answer) - 1]


def infer_grant_actions(canonical: Connector, all_connectors: list[Connector]) -> list[str] | None:
    """Pick a "user" (non-owner) permission set, preferring one already used on the canonical connector."""
    for pool in ([canonical], all_connectors):
        sets = {frozenset(p.get("Actions", [])) for c in pool for p in c.permissions if p.get("Actions")}
        if len(sets) < 2:
            continue
        largest = max(sets, key=len)
        smaller = [s for s in sets if len(s) < len(largest)]
        if smaller:
            return sorted(min(smaller, key=len))
    return None


# --------------------------------------------------------------------------------------
# Access to the canonical connector
# --------------------------------------------------------------------------------------
class AccessChecker:
    """Decides whether a principal can use a connector: listed directly, or through a Quick group."""

    def __init__(self, api: QuickApi):
        self.api = api
        self._groups: dict[str, set[str] | None] = {}

    def groups_of(self, principal: str) -> set[str] | None:
        """Group ARNs the user belongs to; empty for groups; None if membership could not be read."""
        m = USER_ARN.match(principal)
        if not m:
            return set()
        if principal not in self._groups:
            try:
                groups = self.api.list_user_groups(m["name"], m["ns"])
                self._groups[principal] = {g["Arn"] for g in groups if g.get("Arn")}
            except (ClientError, RuntimeError) as exc:  # RuntimeError: pagination did not advance
                LOG.warning("Could not read group membership of %s: %s", principal, exc)
                self._groups[principal] = None
        return self._groups[principal]

    def check(self, principal: str, permissions: list[dict]) -> tuple[bool, str]:
        listed = {p["Principal"] for p in permissions}
        if principal in listed:
            return True, "direct permission"
        groups = self.groups_of(principal)
        if groups is None:
            return False, "no direct permission, and group membership could not be read"
        via = sorted(groups & listed)
        if via:
            return True, "through group " + ", ".join(via)
        return False, "no direct permission and not in any group that has access"


def required_principals(dup: Connector, refs: list[Reference], scope: str) -> set[str]:
    """Users and groups that must be able to use the canonical connector before dup can be retired."""
    principals = set(dup.principals())
    if scope == "all":
        principals |= {o for r in refs for o in r.owners if o.startswith("arn:")}
    # Only Quick users and groups can hold a connector permission (no namespace or account root).
    return {p for p in principals if GRANTABLE_PRINCIPAL.match(p)}


def grant_plan(args: argparse.Namespace, canon: Connector, all_connectors: list[Connector]) -> tuple[list[str] | None, str]:
    """(actions to grant, reason when granting is not possible)."""
    if args.grant_scope == "none":
        return None, "--grant-scope none: the script does not share the canonical connector"
    actions = args.grant_actions or infer_grant_actions(canon, all_connectors)
    if not actions:
        return None, "could not infer a user permission set; pass --grant-actions"
    if len(actions) > MAX_ACTIONS_PER_PERMISSION:
        return None, f"{len(actions)} actions given; a permission takes at most {MAX_ACTIONS_PER_PERMISSION}"
    return list(actions), ""


ACCESS_ISSUE_FIELDS = ["user", "status", "reason", "duplicate_connector_id", "duplicate_connector_name",
                       "connector_type", "canonical_connector_id", "canonical_connector_name",
                       "agents", "flows", "spaces"]


def access_issue(principal: str, status: str, reason: str, dup: Connector, canon: Connector,
                 refs: list[Reference]) -> dict:
    def names(kind: str) -> str:
        return "; ".join(f"{r.name} ({r.resource_id})" for r in refs if r.kind == kind)

    return {"user": principal, "status": status, "reason": reason,
            "duplicate_connector_id": dup.id, "duplicate_connector_name": dup.name, "connector_type": dup.type,
            "canonical_connector_id": canon.id, "canonical_connector_name": canon.name,
            "agents": names("agent"), "flows": names("flow"), "spaces": names("space")}


def preview_access(groups: list[Group], usage: "Usage", access: AccessChecker, args: argparse.Namespace,
                   all_connectors: list[Connector]) -> list[dict]:
    """Dry run: users who have no access to the canonical connector today, and what --apply would do."""
    rows = []
    for g in groups:
        if not g.canonical:
            continue
        actions, why_not = grant_plan(args, g.canonical, all_connectors)
        for dup in g.duplicates:
            refs = usage.of(dup)
            for p in sorted(required_principals(dup, refs, args.grant_scope)):
                ok, how = access.check(p, g.canonical.permissions)
                if ok:
                    continue
                if actions:
                    rows.append(access_issue(p, "will be granted", f"{how}; --apply grants {', '.join(actions)}",
                                             dup, g.canonical, refs))
                else:
                    rows.append(access_issue(p, "will be skipped", f"{how}; {why_not}", dup, g.canonical, refs))
    return rows


def write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_access_issues(path: Path, rows: list[dict]) -> None:
    write_csv(path, ACCESS_ISSUE_FIELDS, rows)


FLOWS_FILE = "flows_needing_action.csv"
FLOW_ISSUE_FIELDS = ["flow_id", "flow_name", "owners", "status", "reason", "draft_changes",
                     "copy_flow_id", "copy_flow_name", "duplicate_connector_id", "duplicate_connector_name",
                     "canonical_connector_id", "canonical_connector_name"]


def flow_issue(ref: Reference, status: str, reason: str, dup: Connector, canon: Connector,
               draft_changes: list[str] | None = None, copy_info: dict | None = None) -> dict:
    return {"flow_id": ref.resource_id, "flow_name": ref.name, "owners": "; ".join(ref.owners),
            "status": status, "reason": reason, "draft_changes": "; ".join(draft_changes or []),
            "copy_flow_id": (copy_info or {}).get("flow_id", ""), "copy_flow_name": (copy_info or {}).get("name", ""),
            "duplicate_connector_id": dup.id, "duplicate_connector_name": dup.name,
            "canonical_connector_id": canon.id, "canonical_connector_name": canon.name}


def preview_flow_issues(groups: list[Group], usage: "Usage") -> list[dict]:
    """Dry run: flows that --apply would leave unchanged, or that get a draft copy the owner must review."""
    rows = []
    for g in groups:
        if not g.canonical:
            continue
        for dup in g.duplicates:
            for ref in usage.of(dup):
                if ref.kind != "flow":
                    continue
                if ref.detail.get("blocked_by"):
                    rows.append(flow_issue(ref, "will be skipped", "; ".join(ref.detail["blocked_by"]),
                                           dup, g.canonical, ref.detail.get("draft_changes")))
                elif ref.detail.get("plan") == "copy-draft":
                    rows.append(flow_issue(ref, "draft copy will be created",
                                           "draft has unpublished edits: they go into a copy visible to the owners "
                                           "only; the original is updated from its published version",
                                           dup, g.canonical, ref.detail.get("draft_changes")))
    return rows


# --------------------------------------------------------------------------------------
# Execution
# --------------------------------------------------------------------------------------
class Runner:
    def __init__(self, api: QuickApi, args: argparse.Namespace, usage: Usage, all_connectors: list[Connector],
                 backup_dir: Path, access: AccessChecker):
        self.api = api
        self.args = args
        self.usage = usage
        self.all_connectors = all_connectors
        self.backup_dir = backup_dir
        self.access = access
        self.access_issues: list[dict] = []  # users left without access; their duplicate is skipped
        self.flow_issues: list[dict] = []  # flows left unchanged; their duplicate is kept
        self.flow_copies: dict[str, dict] = {}  # original flow id -> draft copy created for it

    def backup(self, kind: str, rid: str, payload: Any) -> None:
        path = self.backup_dir / kind / f"{rid}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str))

    def migrate(self, canon: Connector, dup: Connector) -> dict:
        result = {"duplicate": dup.summary(), "canonical_id": canon.id, "steps": [], "deleted": False}
        failed = False

        def step(kind: str, target: str, status: str, message: str = "") -> None:
            nonlocal failed
            failed = failed or status in ("failed", "blocked")
            result["steps"].append({"kind": kind, "target": target, "status": status, "message": message})
            if kind == "flow" and status == "failed":
                ref = next((r for r in refs if r.kind == "flow" and r.resource_id == target), None)
                if ref:
                    self.flow_issues.append(flow_issue(ref, "skipped", message, dup, canon))
            log = (LOG.error if status in ("failed", "blocked")
                   else LOG.warning if status == "warning" else LOG.info)
            log("[%s -> %s] %s %s: %s %s", dup.id, canon.id, kind, target, status, message)

        self.backup("connectors", dup.id, {"connector": dup.summary(), "permissions": dup.permissions,
                                           "endpoints": dup.endpoints, "enabled_actions": dup.enabled_actions})
        refs = self.usage.of(dup)

        # Gate: every user of the duplicate must be able to use the canonical connector. Otherwise this
        # duplicate (that user's connector) and everything that uses it are left exactly as they are.
        issues = self._ensure_access(canon, dup, refs, step)
        if issues:
            self.access_issues.extend(issues)
            step("access", dup.id, "blocked",
                 f"{len(issues)} user(s) cannot use the canonical connector ({', '.join(i['user'] for i in issues)}); "
                 f"duplicate and its agents/flows/spaces left unchanged, see {ACCESS_ISSUES_FILE}")
            step("delete", dup.id, "skipped", "access check failed; duplicate kept")
            result["ok"] = False
            return result

        for ref in refs:
            try:
                if ref.kind == "agent":
                    self._agent(ref, canon, dup, step)
                elif ref.kind == "flow":
                    self._flow(ref, canon, dup, step)
                elif ref.kind == "space":
                    self._space(ref, canon, dup, step)
            except ClientError as exc:
                step(ref.kind, ref.resource_id, "failed", str(exc))

        if failed:
            step("delete", dup.id, "skipped", "earlier step failed; duplicate kept")
        elif self.args.keep_duplicates:
            step("delete", dup.id, "skipped", "--keep-duplicates")
        else:
            try:
                self.api.delete_connector(dup.id)
            except ClientError as exc:
                step("delete", dup.id, "failed", str(exc))
            else:
                # The call returning is not proof: confirm the connector is really gone.
                gone, state = self._wait_deleted(dup.id)
                if gone:
                    result["deleted"] = True
                    step("delete", dup.id, "done", f"{dup.name} ({state})")
                else:
                    step("delete", dup.id, "failed", f"delete accepted but connector still present: {state}")
        result["ok"] = not failed
        return result

    def _wait_deleted(self, cid: str, timeout: int = 60) -> tuple[bool, str]:
        """True once DescribeActionConnector says not found or status DELETED."""
        deadline = time.time() + timeout
        while True:
            try:
                status = self.api.describe_connector(cid).get("Status")
            except ClientError as exc:
                if error_code(exc) == "ResourceNotFoundException":
                    return True, "not found"
                return False, f"could not verify: {exc}"
            if status == "DELETED":
                return True, "status DELETED"
            if time.time() > deadline:
                return False, f"status {status} after {timeout}s"
            time.sleep(3)

    def _ensure_access(self, canon: Connector, dup: Connector, refs: list[Reference], step) -> list[dict]:
        """Grant what is missing, read the permissions back, and return the users still without access."""
        required = required_principals(dup, refs, self.args.grant_scope)
        ignored = sorted(set(dup.principals()) - required)
        if ignored:
            step("access", canon.id, "warning",
                 "not a Quick user or group ARN, cannot hold a connector permission: " + ", ".join(ignored))
        missing = sorted(p for p in required if not self.access.check(p, canon.permissions)[0])
        if not missing:
            step("access", canon.id, "done", f"all {len(required)} user(s) of the duplicate already have access")
            return []

        actions, why_not = grant_plan(self.args, canon, self.all_connectors)
        grant_error = ""
        if actions:
            grants = [{"Principal": p, "Actions": actions} for p in missing]
            try:
                self.api.grant_connector_permissions(canon.id, grants)
                step("grant", canon.id, "done", f"{len(grants)} principal(s): {', '.join(missing)}")
            except ClientError as exc:
                grant_error = f"grant failed: {exc}"
                step("grant", canon.id, "warning", grant_error)
        else:
            step("grant", canon.id, "skipped", why_not)

        # Read back the real permissions: the grant call returning is not proof of access.
        try:
            canon.permissions = self.api.connector_permissions(canon.id)
        except ClientError as exc:
            reason = f"could not read canonical connector permissions back: {exc}"
            return [access_issue(p, "skipped", reason, dup, canon, refs) for p in missing]

        issues = []
        for p in missing:
            ok, how = self.access.check(p, canon.permissions)
            if ok:
                continue
            reason = "; ".join(x for x in (how, why_not, grant_error) if x)
            if actions and not grant_error:
                reason += "; granted but not present when read back"
            issues.append(access_issue(p, "skipped", reason, dup, canon, refs))
        if not issues:
            step("access", canon.id, "done", f"verified access for {len(missing)} newly granted principal(s)")
        return issues

    def _guard_permissions(self, kind: str, rid: str, before: list[dict], read: Callable[[], list[dict]],
                           update: Callable[[list[dict], list[dict]], None], step) -> bool:
        """After an update, make the resource's permissions (ownership and sharing) identical to before.

        Returns False (and records a failure) when they differ and could not be restored.
        """
        try:
            grant, revoke = permission_diff(before, read())
        except ClientError as exc:
            step(kind, rid, "failed", f"could not read permissions back to check ownership: {exc}")
            return False
        if not grant and not revoke:
            return True
        changes = (f"removed {[(g['Principal'], g['Actions']) for g in grant]}, "
                   f"added {[(r['Principal'], r['Actions']) for r in revoke]}")
        try:
            update(grant, revoke)
            grant, revoke = permission_diff(before, read())
        except ClientError as exc:
            step(kind, rid, "failed", f"permissions changed by the update ({changes}) and restore failed: {exc}")
            return False
        if grant or revoke:
            step(kind, rid, "failed", f"permissions changed by the update ({changes}) and could not be restored")
            return False
        step(kind, rid, "warning", f"permissions changed by the update and were restored: {changes}")
        return True

    def _agent(self, ref: Reference, canon: Connector, dup: Connector, step) -> None:
        agent = self.api.describe_agent(ref.resource_id)
        permissions = self.api.agent_permissions(ref.resource_id)
        self.backup("agents", ref.resource_id, {"agent": agent, "permissions": permissions})
        linked = agent.get("ActionConnectors") or []
        to_remove = [a for a in linked if dup.matches(a)]
        if not to_remove:
            step("agent", ref.resource_id, "skipped", "no longer uses the duplicate")
            return
        kwargs: dict[str, Any] = {"AgentId": ref.resource_id, "Name": agent["Name"],
                                  "ActionConnectorsToRemove": to_remove}
        if not any(canon.matches(a) for a in linked):
            kwargs["ActionConnectorsToAdd"] = [canon.arn]
        # Pass the current values through so an update never blanks them.
        for key in ("Description", "IconId", "StarterPrompts", "WelcomeMessage"):
            if agent.get(key):
                kwargs[key] = agent[key]
        prompt = agent.get("CustomPromptInterface") or {}
        if all(prompt.get(k) for k in ("ModelProfileId", "SubscriptionId", "QbsAwsAccountId")):
            kwargs["CustomPromptInput"] = {"ExistingPrompt": {k: prompt[k] for k in
                                                              ("ModelProfileId", "SubscriptionId", "QbsAwsAccountId")}}
        resp = self.api.update_agent(**kwargs)
        failures = (resp.get("FailedToAddActionConnectors") or []) + (resp.get("FailedToRemoveActionConnectors") or [])
        if failures:
            step("agent", ref.resource_id, "failed", json.dumps(failures, default=str))
            return
        agent = self._wait_agent(ref.resource_id)
        if not self._guard_permissions("agent", ref.resource_id, permissions,
                                       lambda: self.api.agent_permissions(ref.resource_id),
                                       lambda g, r: self.api.update_agent_permissions(ref.resource_id, g, r), step):
            return
        still = [a for a in agent.get("ActionConnectors") or [] if dup.matches(a)]
        if agent.get("AgentStatus") == "FAILED" or still:
            step("agent", ref.resource_id, "failed",
                 f"status={agent.get('AgentStatus')} error={agent.get('ErrorMessage')} still_linked={still}")
        else:
            step("agent", ref.resource_id, "done", ref.name)

    def _wait_agent(self, agent_id: str, timeout: int = 180) -> dict:
        deadline = time.time() + timeout
        while True:
            agent = self.api.describe_agent(agent_id)
            if agent.get("AgentStatus") not in ("UPDATING", "CREATING") or time.time() > deadline:
                return agent
            time.sleep(3)

    def _flow(self, ref: Reference, canon: Connector, dup: Connector, step) -> None:
        fid = ref.resource_id
        versions = {s: f for s in ALL_FLOW_STATES if (f := self.api.describe_flow(fid, s)) is not None}
        states = {s: f.get("FlowDefinition") for s, f in versions.items()}
        permissions = self.api.flow_permissions(fid)
        # Full versions (definition, CreatedBy, LastUpdatedBy, ...) and the sharing, as they were.
        self.backup("flows", fid, {"versions": versions, "permissions": permissions})

        def guard() -> bool:
            return self._guard_permissions("flow", fid, permissions, lambda: self.api.flow_permissions(fid),
                                           lambda g, r: self.api.update_flow_permissions(fid, g, r), step)

        if not any(references_connector(d, dup) for d in states.values()):
            step("flow", fid, "skipped", "no longer uses the duplicate")
            return

        plan = plan_flow(states, self.args.flow_divergence)
        if plan.mode is None:
            step("flow", fid, "failed", plan.reason)
            return
        rewritten, problems = plan_rewrites(plan, dup, canon)
        if problems:
            step("flow", fid, "failed", "; ".join(problems))
            return

        if plan.mode == "single":
            if self._update_original(fid, rewritten["source"], plan.source, dup, guard, step):
                step("flow", fid, "done", f"{ref.name}: connector steps re-pointed")
            return

        # copy-draft: keep the user's unpublished edits in a copy first, then update the original.
        copy_info = self._create_draft_copy(ref, permissions, rewritten["draft"], plan, dup, canon, step)
        if copy_info is None:
            return  # copy failed its checks and was removed; the original is untouched
        self.flow_copies[fid] = copy_info
        if self._update_original(fid, rewritten["published"], plan.published, dup, guard, step):
            copy_info["original_migrated"] = True
            step("flow", fid, "done", f"{ref.name}: published version re-pointed; unpublished draft edits kept in "
                 f"{copy_info['name']!r} ({copy_info['flow_id']}), visible to the owners only")
            self.flow_issues.append(flow_issue(
                ref, "draft copied", "the owner should review the copy, carry the edits over, then delete it",
                dup, canon, plan.draft_changes, copy_info))
        else:
            copy_info["original_migrated"] = False
            step("flow-copy", copy_info["flow_id"], "warning",
                 "original flow not migrated; the copy is kept because it holds the unpublished draft edits")
            self.flow_issues.append(flow_issue(
                ref, "draft copied, original not migrated",
                "the original update failed; the copy holds the draft edits (see results.json)",
                dup, canon, plan.draft_changes, copy_info))

    def _update_original(self, fid: str, new_def: Any, original_def: Any, dup: Connector,
                         guard: Callable[[], bool], step) -> bool:
        """UpdateFlow (writes draft and published), read back, roll back on mismatch. True when verified."""
        expected = sorted(s.signature() for s in connector_steps(new_def))
        self.api.update_flow(fid, new_def)
        permissions_ok = guard()

        # Read back both versions: every connector step must keep its action and point at the canonical.
        mismatches = []
        for state in FLOW_STATES:
            after = self._flow_def(fid, state)
            if after is None:
                continue
            actual = sorted(s.signature() for s in connector_steps(after))
            if actual != expected:
                mismatches.append(f"{state}: expected {expected}, got {actual}")
            elif references_connector(after, dup):
                mismatches.append(f"{state}: still references the duplicate")
        if not mismatches:
            return permissions_ok

        # The service changed or dropped something: put the original definition back.
        try:
            self.api.update_flow(fid, original_def)
            guard()
            step("flow", fid, "failed", "connector steps changed after update, original definition restored: "
                 + " | ".join(mismatches))
        except ClientError as exc:
            step("flow", fid, "failed", f"connector steps changed after update AND rollback failed ({exc}); "
                 f"restore from {self.backup_dir / 'flows' / (fid + '.json')}: " + " | ".join(mismatches))
        return False

    def _create_draft_copy(self, ref: Reference, permissions: list[dict], new_draft: Any, plan: FlowPlan,
                           dup: Connector, canon: Connector, step) -> dict | None:
        """Create a flow holding the user's re-pointed draft, shared with the original's owners only.

        CreateFlow auto-publishes, so "owners only" is what keeps the copy private. The copy is read back
        (connector steps, step IDs used by @references in prompts, permissions); if anything is off it's
        deleted again and None is returned.
        """
        fid = ref.resource_id
        owners = owner_permissions(permissions)
        if not owners:
            step("flow", fid, "failed", "draft has unpublished edits, but no owner was found to share a copy with")
            return None
        name = draft_copy_name(ref.name)
        description = draft_copy_description(ref.name, fid, dup, canon)
        try:
            cid = self.api.create_flow(name, description, new_draft, owners)
        except ClientError as exc:
            step("flow", fid, "failed", f"could not create the draft copy: {exc}")
            return None
        self.backup("flow-copies", cid, {"source_flow_id": fid, "definition": new_draft, "permissions": owners})

        problems = []
        expected_sig = sorted(s.signature() for s in connector_steps(new_draft))
        expected_ids = [s.get("StepId") for s in _steps(new_draft)]
        for state in FLOW_STATES:
            got = self._flow_def(cid, state)
            if got is None:
                continue
            if sorted(s.signature() for s in connector_steps(got)) != expected_sig:
                problems.append(f"{state}: connector steps differ from what was sent")
            if [s.get("StepId") for s in _steps(got)] != expected_ids:
                problems.append(f"{state}: step IDs changed, so @step references in prompts would break")
            if references_connector(got, dup):
                problems.append(f"{state}: still references the duplicate")
        if not self._guard_permissions("flow-copy", cid, owners, lambda: self.api.flow_permissions(cid),
                                       lambda g, r: self.api.update_flow_permissions(cid, g, r), step):
            problems.append("could not make the copy visible to the owners only")

        if problems:
            try:
                self.api.delete_flow(cid)
                step("flow", fid, "failed", "draft copy failed its checks and was deleted; original left unchanged: "
                     + "; ".join(problems))
            except ClientError as exc:
                step("flow", fid, "failed", f"draft copy {cid} failed its checks ({'; '.join(problems)}) and could "
                     f"not be deleted ({exc}); delete it by hand. Original left unchanged")
            return None
        step("flow-copy", cid, "done", f"{name!r} created from the draft of {ref.name!r}, shared with "
             + ", ".join(p["Principal"] for p in owners))
        return {"flow_id": cid, "name": name, "owners": [p["Principal"] for p in owners],
                "draft_changes": plan.draft_changes}

    def _flow_def(self, fid: str, state: str) -> Any:
        flow = self.api.describe_flow(fid, state)
        return None if flow is None else flow.get("FlowDefinition")

    def _space(self, ref: Reference, canon: Connector, dup: Connector, step) -> None:
        sid = ref.resource_id
        resources = self.api.space_resources(sid)
        self.backup("spaces", sid, resources)
        arns = [r.get("ResourceDetails", {}).get("resourceArn", "")
                for r in resources if r.get("ResourceType") == "ACTION_CONNECTOR"]
        remove = [a for a in arns if a and dup.matches(a)]
        if not remove:
            step("space", sid, "skipped", "no longer uses the duplicate")
            return
        add = [] if any(canon.matches(a) for a in arns if a) else [canon.arn]
        resp = self.api.update_space_resources(sid, add, remove)
        if resp.get("FailedResourceOperations"):
            step("space", sid, "failed", json.dumps(resp["FailedResourceOperations"], default=str))
            return
        after = [r.get("ResourceDetails", {}).get("resourceArn", "") for r in self.api.space_resources(sid)]
        if any(dup.matches(a) for a in after if a):
            step("space", sid, "failed", "duplicate still attached after update")
        else:
            step("space", sid, "done", ref.name)


# --------------------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------------------
def users_view(groups: list[Group], usage: Usage) -> dict[str, dict]:
    """Per user: the duplicate connectors they own and the agents/flows/spaces that use them."""
    view: dict[str, dict] = {}
    for g in groups:
        for dup in g.candidates:
            for owner in dup.owners() or ["(unknown owner)"]:
                view.setdefault(owner, {"connectors": [], "agents": [], "flows": [], "spaces": []})
                view[owner]["connectors"].append(f"{dup.name} ({dup.type}, {dup.id})")
            for ref in usage.of(dup):
                for owner in ref.owners:
                    entry = view.setdefault(owner, {"connectors": [], "agents": [], "flows": [], "spaces": []})
                    entry[ref.kind + "s"].append(f"{ref.name} ({ref.resource_id}) uses {dup.name}")
    return view


def annotate_flows(groups: list[Group], usage: Usage, divergence: str) -> None:
    """Attach each flow's connector steps, and anything that would block its migration, to its reference."""
    for g in groups:
        for dup in g.candidates:
            for ref in usage.of(dup):
                if ref.kind != "flow":
                    continue
                states = usage.flow_defs.get(ref.resource_id, {})
                source = states.get("PUBLISHED", states.get("DRAFT"))
                ref.detail["connector_steps"] = [
                    {"step_id": s.step_id, "title": s.title, "actionId": s.config.get("actionId")}
                    for s in connector_steps(source) if dup.matches(str(s.config.get("actionConnectorId") or ""))
                ]
                plan = plan_flow(states, divergence)
                ref.detail["plan"] = plan.mode
                ref.detail["draft_changes"] = plan.draft_changes
                if g.canonical:
                    ref.detail["blocked_by"] = flow_preflight(usage, ref.resource_id, dup, g.canonical, divergence)


def print_plan(groups: list[Group], generic: list[Connector], usage: Usage) -> None:
    print("\n=== Workspace connector consolidation plan ===")
    for g in groups:
        print(f"\n{g.type}: {len(g.connectors)} connector(s)")
        if g.skip_reason:
            print(f"  SKIPPED: {g.skip_reason}")
        if g.canonical:
            print(f"  canonical : {g.canonical.name!r} ({g.canonical.id})")
        blocked_any = False
        role = "duplicate" if g.canonical else "candidate"
        for dup in g.candidates:
            refs = usage.of(dup)
            print(f"  {role} : {dup.name!r} ({dup.id}) owners={', '.join(dup.owners()) or '-'} "
                  f"status={dup.status} created={dup.created}")
            for r in refs:
                print(f"      - {r.kind:5} {r.name!r} ({r.resource_id}) owners={', '.join(r.owners)}")
                for s in r.detail.get("connector_steps", []):
                    print(f"          step {s['title']!r}: action {s['actionId']}")
                if r.detail.get("draft_changes"):
                    print(f"          draft has unpublished edits: {'; '.join(r.detail['draft_changes'])}")
                    if r.detail.get("plan") == "copy-draft" and not r.detail.get("blocked_by"):
                        print("          -> draft copied to a new flow visible to the owners only; original "
                              "updated from its published version")
                for problem in r.detail.get("blocked_by", []):
                    blocked_any = True
                    print(f"          ! BLOCKED: {problem}")
            if not refs:
                print("      - not used by any visible agent, flow or space")
        if blocked_any:
            print("  ! Blocked flows are left unchanged and keep their duplicate. Enable the listed actions on the "
                  "canonical connector, then run again.")
    if generic:
        print("\nCustom GENERIC_HTTP connectors that call workspace APIs (review manually, not migrated):")
        for c in generic:
            print(f"  - {c.name!r} ({c.id}) owners={', '.join(c.owners()) or '-'} endpoints={', '.join(c.endpoints)}")
    view = users_view(groups, usage)
    if view:
        print("\nImpact per user:")
        for user, items in sorted(view.items()):
            print(f"  {user}")
            for kind in ("connectors", "agents", "flows", "spaces"):
                for item in items[kind]:
                    print(f"      {kind[:-1]:9} {item}")
    if usage.errors:
        print(f"\n{len(usage.errors)} resource(s) could not be read; see the report for details.")
    print()


# --------------------------------------------------------------------------------------
# Messages to the impacted users (French and English)
# --------------------------------------------------------------------------------------
USER_MESSAGES_CSV = "user_messages.csv"
USER_MESSAGES_MD = "user_messages.md"
USER_MESSAGE_FIELDS = ["user", "email", "subject_fr", "body_fr", "subject_en", "body_en", "screenshots",
                       "admin_notes"]
KIND_LABEL = {"en": {"agent": "Agent", "flow": "Flow", "space": "Space"},
              "fr": {"agent": "Agent", "flow": "Flow", "space": "Espace"}}
PENDING_TEXT = {
    "access": {
        "en": "you don't have access to the shared connector yet. Your IT team will fix this and finish the change.",
        "fr": "vous n'avez pas encore accès au connecteur partagé. Votre équipe IT va corriger cela et terminer "
              "la modification."},
    "pending_approval": {
        "en": "a version is waiting for approval. Once it's approved or rejected, your IT team will finish the "
              "change.",
        "fr": "une version est en attente d'approbation. Une fois approuvée ou rejetée, votre équipe IT terminera "
              "la modification."},
    "action_not_enabled": {
        "en": "it uses an action that isn't enabled on the shared connector yet. Your IT team will enable it and "
              "finish the change.",
        "fr": "il utilise une action qui n'est pas encore activée sur le connecteur partagé. Votre équipe IT va "
              "l'activer et terminer la modification."},
    "draft_edits": {
        "en": "it has unpublished draft changes. Publish or discard them, then your IT team will finish the change.",
        "fr": "il contient des modifications de brouillon non publiées. Publiez-les ou abandonnez-les, puis votre "
              "équipe IT terminera la modification."},
    "other": {
        "en": "a technical check failed. Your IT team is looking into it; nothing was changed on this item.",
        "fr": "une vérification technique a échoué. Votre équipe IT s'en occupe ; rien n'a été modifié sur cet "
              "élément."},
}


def pending_category(message: str) -> str:
    m = message.lower()
    if "pending approval" in m:
        return "pending_approval"
    if "not enabled on the canonical" in m:
        return "action_not_enabled"
    if "unpublished edits" in m and "skip" in m:
        return "draft_edits"
    return "other"


@dataclass
class UserImpact:
    user: str
    connectors: list[dict] = field(default_factory=list)  # {"name", "type", "canonical", "deleted", "category"}
    switched: list[tuple[str, str, str]] = field(default_factory=list)  # (kind, name, canonical name)
    copies: list[dict] = field(default_factory=list)  # {"flow", "copy", "canonical", "original_migrated"}
    pending: list[dict] = field(default_factory=list)  # {"kind", "name", "category"}
    sign_in: set = field(default_factory=set)  # canonical connector names to sign in to
    notes: list[str] = field(default_factory=list)  # technical details for the admin


def _users_only(principals: list[str]) -> list[str]:
    return sorted({p for p in principals if USER_ARN.match(p)})


def collect_impacts(results: list[dict], runner: "Runner", groups: list[Group], usage: "Usage",
                    keep_duplicates: bool) -> dict[str, UserImpact]:
    """Per Quick user: what --apply did to their connectors, agents, flows and spaces."""
    by_dup = {d.id: (d, g.canonical) for g in groups if g.canonical for d in g.duplicates}
    impacts: dict[str, UserImpact] = {}

    def of(user: str) -> UserImpact:
        return impacts.setdefault(user, UserImpact(user))

    for res in results:
        dup, canon = by_dup.get(res["duplicate"]["id"], (None, None))
        if dup is None:
            continue
        steps = res["steps"]
        blocked = any(s["kind"] == "access" and s["status"] == "blocked" for s in steps)
        if res["deleted"]:
            category = ""
        elif blocked:
            category = "access"
        elif keep_duplicates:
            category = "kept"
        else:
            category = "in_use"
        for user in _users_only(dup.owners()):
            of(user).connectors.append({"name": dup.name, "type": dup.type, "canonical": canon.name,
                                        "deleted": res["deleted"], "category": category})
            if res["deleted"]:
                of(user).sign_in.add(canon.name)
        for ref in usage.of(dup):
            mine = [s for s in steps if s["target"] == ref.resource_id and s["kind"] == ref.kind]
            failed = [s for s in mine if s["status"] == "failed"]
            for user in _users_only(ref.owners):
                imp = of(user)
                if blocked:
                    imp.pending.append({"kind": ref.kind, "name": ref.name, "category": "access"})
                elif failed:
                    imp.pending.append({"kind": ref.kind, "name": ref.name,
                                        "category": pending_category(failed[0]["message"])})
                    imp.notes.append(f"{ref.kind} {ref.name} ({ref.resource_id}): {failed[0]['message']}")
                elif any(s["status"] == "done" for s in mine):
                    imp.switched.append((ref.kind, ref.name, canon.name))
                    imp.sign_in.add(canon.name)
                copy_info = runner.flow_copies.get(ref.resource_id) if ref.kind == "flow" else None
                if copy_info and user in copy_info["owners"]:
                    imp.copies.append({"flow": ref.name, "copy": copy_info["name"], "canonical": canon.name,
                                       "original_migrated": copy_info.get("original_migrated", False)})
                    imp.notes.append(f"draft copy {copy_info['flow_id']} of {ref.resource_id}: "
                                     + "; ".join(copy_info.get("draft_changes") or []))
    for issue in runner.access_issues:
        if issue["user"] in impacts:
            impacts[issue["user"]].notes.append(f"no access to {issue['canonical_connector_name']}: {issue['reason']}")
    return {u: i for u, i in impacts.items() if i.connectors or i.switched or i.copies or i.pending}


# Message texts. Each entry: lang -> (after --apply, planned in a dry run). {q} quotes a name.
MSG = {
    "intro": {
        "fr": ("Votre équipe IT a mis en place des connecteurs partagés dans AWS Quick, pour que les agents et "
               "les flows puissent être partagés sans partager de connecteur personnel. Le {day}, les "
               "modifications suivantes ont été faites sur votre compte :",
               "Votre équipe IT met en place des connecteurs partagés dans AWS Quick, pour que les agents et "
               "les flows puissent être partagés sans partager de connecteur personnel. Le {day}, les "
               "modifications suivantes seront faites sur votre compte :"),
        "en": ("Your IT team has set up shared connectors in AWS Quick, so that agents and flows can be shared "
               "without sharing a personal connector. On {day}, the following changes were made in your account:",
               "Your IT team is setting up shared connectors in AWS Quick, so that agents and flows can be "
               "shared without sharing a personal connector. On {day}, the following changes will be made in "
               "your account:")},
    "connectors": {"fr": ("Vos connecteurs",) * 2, "en": ("Your connectors",) * 2},
    "conn_deleted": {
        "fr": ("- {name} ({type}) a été remplacé par le connecteur partagé {canon}, puis supprimé.",
               "- {name} ({type}) sera remplacé par le connecteur partagé {canon}, puis supprimé."),
        "en": ("- {name} ({type}) was replaced by the shared connector {canon}, then deleted.",
               "- {name} ({type}) will be replaced by the shared connector {canon}, then deleted.")},
    "conn_access": {
        "fr": ("- {name} ({type}) n'a pas été modifié : {why}", "- {name} ({type}) ne sera pas encore modifié : {why}"),
        "en": ("- {name} ({type}) was not changed: {why}", "- {name} ({type}) won't be changed yet: {why}")},
    "conn_kept": {
        "fr": ("- {name} ({type}) est conservé pour l'instant ; il sera supprimé plus tard. Utilisez désormais "
               "{canon}.",
               "- {name} ({type}) sera conservé pour l'instant, puis supprimé plus tard. Utilisez {canon} dès que "
               "la modification est faite."),
        "en": ("- {name} ({type}) is kept for now and will be deleted later. Please use {canon} from now on.",
               "- {name} ({type}) will be kept for now and deleted later. Please use {canon} once the change is "
               "done.")},
    "conn_in_use": {
        "fr": ("- {name} ({type}) est conservé tant qu'un élément listé plus bas l'utilise encore.",
               "- {name} ({type}) sera conservé tant qu'un élément listé plus bas l'utilise encore."),
        "en": ("- {name} ({type}) is kept while an item listed below still uses it.",
               "- {name} ({type}) will be kept while an item listed below still uses it.")},
    "switched": {
        "fr": ("Éléments qui utilisent désormais le connecteur partagé",
               "Éléments qui utiliseront le connecteur partagé"),
        "en": ("Items that now use the shared connector", "Items that will use the shared connector")},
    "copies": {"fr": ("Vos modifications de brouillon non publiées",) * 2,
               "en": ("Your unpublished draft changes",) * 2},
    "copy_ok": {
        "fr": ("- Le flow {flow} avait des modifications de brouillon non publiées. Sa version publiée utilise "
               "désormais {canon}. Vos modifications sont conservées dans un nouveau flow, {copy}, que vous seul "
               "pouvez voir. Poursuivez votre travail dans cette copie, ou reportez vos modifications dans le flow "
               "original, puis supprimez la copie.",
               "- Le flow {flow} contient des modifications de brouillon non publiées. Sa version publiée "
               "utilisera {canon}. Vos modifications seront conservées dans un nouveau flow, {copy}, que vous seul "
               "pourrez voir. Vous pourrez poursuivre votre travail dans cette copie, ou reporter vos "
               "modifications dans le flow original, puis supprimer la copie. Si vous préférez éviter cette copie, "
               "publiez ou abandonnez votre brouillon avant le {day}."),
        "en": ("- Flow {flow} had unpublished draft changes. Its published version now uses {canon}. Your changes "
               "are kept in a new flow, {copy}, which only you can see. Continue your work in that copy, or copy "
               "your changes into the original flow, then delete the copy.",
               "- Flow {flow} has unpublished draft changes. Its published version will use {canon}. Your changes "
               "will be kept in a new flow, {copy}, which only you will be able to see. You can then continue your "
               "work in that copy, or copy your changes into the original flow, then delete the copy. To avoid "
               "the copy, publish or discard your draft before {day}.")},
    "copy_orig_failed": {
        "fr": ("- Le flow {flow} n'a pas encore été modifié. Vos modifications de brouillon sont aussi "
               "enregistrées dans {copy}, que vous seul pouvez voir.",) * 2,
        "en": ("- Flow {flow} hasn't been changed yet. Your draft changes are also saved in {copy}, which only you "
               "can see.",) * 2},
    "pending": {"fr": ("Pas encore modifié", "Ne sera pas encore modifié"),
                "en": ("Not changed yet", "Won't be changed yet")},
    "todo": {"fr": ("Ce que vous devez faire",) * 2, "en": ("What you need to do",) * 2},
    "sign_in": {
        "fr": ("- Connectez-vous une fois au connecteur partagé {names} : dans AWS Quick, ouvrez le menu "
               "« More » puis « Connectors », sélectionnez le connecteur, puis cliquez sur « Sign in » (libellés "
               "de la console en anglais) et connectez-vous avec votre compte. Tant que ce n'est pas fait, les "
               "agents et flows qui l'utilisent ne peuvent pas accéder à vos données.",
               "- Après la modification, connectez-vous une fois au connecteur partagé {names} : dans AWS "
               "Quick, ouvrez le menu « More » puis « Connectors », sélectionnez le connecteur, puis cliquez sur "
               "« Sign in » (libellés de la console en anglais) et connectez-vous avec votre compte. Tant que ce "
               "n'est pas fait, les agents et flows qui l'utilisent ne pourront pas accéder à vos données."),
        "en": ("- Sign in once to the shared connector {names}: in AWS Quick, open More > Connectors, select "
               "the connector, choose Sign in and sign in with your account. Until you do, agents and flows that "
               "use it can't access your data.",
               "- After the change, sign in once to the shared connector {names}: in AWS Quick, open More > "
               "Connectors, select the connector, choose Sign in and sign in with your account. Until you do, "
               "agents and flows that use it won't be able to access your data.")},
    "review_copies": {
        "fr": ("- Vérifiez vos copies de brouillon (voir ci-dessus).",
               "- Publiez ou abandonnez votre brouillon avant le {day}, ou vérifiez la copie après la modification "
               "(voir ci-dessus)."),
        "en": ("- Review your draft copies (see above).",
               "- Publish or discard your draft before {day}, or review the copy after the change (see above).")},
    "closing": {"fr": ("Pour toute question, contactez votre équipe IT.",) * 2,
                "en": ("If you have any questions, contact your IT team.",) * 2},
    "subject": {
        "fr": ("AWS Quick : vos connecteurs passent aux connecteurs partagés de l'entreprise",
               "AWS Quick : changement à venir sur vos connecteurs"),
        "en": ("AWS Quick: your connectors are moving to the shared company connectors",
               "AWS Quick: upcoming change to your connectors")},
}
PENDING_OTHER_PLANNED = {
    "fr": "une vérification technique empêche la modification. Votre équipe IT s'en occupe ; rien ne sera modifié "
          "sur cet élément.",
    "en": "a technical check prevents the change. Your IT team is looking into it; nothing will be changed on this "
          "item.",
}
DATE_PLACEHOLDER = {"fr": "[date à compléter]", "en": "[date to fill in]"}

# Screenshots of the Quick console after the change (assets/screenshots/), inserted in the messages.
# A line "[[IMG:<key>]]" in a message body marks where they go.
SCREENSHOT_DIR = Path(__file__).resolve().parent / "assets" / "screenshots"
SCREENSHOTS = {
    "sign_in": [
        ("01-more-connectors-menu.png", {"fr": "Ouvrez « More » puis « Connectors »",
                                         "en": "Open More > Connectors"}),
        ("02-connectors-shared.png", {"fr": "Les connecteurs partagés (exemple : Gmail et Google Calendar)",
                                      "en": "The shared connectors (example: Gmail and Google Calendar)"}),
        ("03-connector-sign-in.png", {"fr": "Sur la page du connecteur, cliquez sur « Sign in »",
                                      "en": "On the connector's page, choose Sign in"}),
    ],
    "draft_copy": [
        ("04-flows-draft-copy.png", {"fr": "Votre copie de brouillon dans la liste « Flows » (exemple)",
                                     "en": "Your draft copy in the Flows list (example)"}),
    ],
}
IMG_MARKER = re.compile(r"^\[\[IMG:(\w+)\]\]$")


def render_body(body: str, lang: str, markdown: bool) -> tuple[str, list[str]]:
    """Replace the screenshot markers: images for Markdown, a pointer to the attachment for plain text.
    Returns (body, screenshot files used). Screenshots that aren't on disk are left out."""
    out, used = [], []
    for line in body.split("\n"):
        m = IMG_MARKER.match(line)
        if not m:
            out.append(line)
            continue
        for name, caption in SCREENSHOTS.get(m.group(1), []):
            if not (SCREENSHOT_DIR / name).is_file():
                continue
            used.append(name)
            if markdown:
                out += ["", f"![{caption[lang]}](screenshots/{name})", f"*{caption[lang]}*", ""]
            elif lang == "fr":
                out.append(f"  (capture jointe : {name} - {caption[lang]})")
            else:
                out.append(f"  (attached screenshot: {name} - {caption[lang]})")
    return "\n".join(out), used


def _message(imp: UserImpact, lang: str, day: str, planned: bool = False) -> tuple[str, str]:
    """Subject and body for one user. planned=True: announcement written from a dry run (future tense)."""
    t = 1 if planned else 0
    fr = lang == "fr"
    q = (lambda s: f"« {s} »") if fr else (lambda s: f'"{s}"')
    kind = KIND_LABEL[lang]

    def txt(key: str, **kw) -> str:
        return MSG[key][lang][t].format(day=day, **kw)

    def pending_text(category: str) -> str:
        if planned and category == "other":
            return PENDING_OTHER_PLANNED[lang]
        return PENDING_TEXT[category][lang]

    lines = ["Bonjour," if fr else "Hello,", "", txt("intro")]
    if imp.connectors:
        lines += ["", txt("connectors")]
        for c in imp.connectors:
            kw = {"name": q(c["name"]), "type": c["type"], "canon": q(c["canonical"])}
            if c["deleted"]:
                lines.append(txt("conn_deleted", **kw))
            elif c["category"] == "access":
                lines.append(txt("conn_access", why=pending_text("access"), **kw))
            elif c["category"] == "kept":
                lines.append(txt("conn_kept", **kw))
            else:
                lines.append(txt("conn_in_use", **kw))
    if imp.switched:
        order = {"agent": 0, "flow": 1, "space": 2}
        lines += ["", txt("switched")]
        lines += [f"- {kind[k]} {q(n)} → {q(c)}"
                  for k, n, c in sorted(imp.switched, key=lambda x: (order.get(x[0], 9), x[1].lower()))]
    if imp.copies:
        lines += ["", txt("copies")]
        for c in imp.copies:
            key = "copy_ok" if c["original_migrated"] else "copy_orig_failed"
            lines.append(txt(key, flow=q(c["flow"]), copy=q(c["copy"]), canon=q(c["canonical"])))
        lines.append("[[IMG:draft_copy]]")
    if imp.pending:
        sep = " : " if fr else ": "
        lines += ["", txt("pending")]
        lines += [f"- {kind[p['kind']]} {q(p['name'])}{sep}{pending_text(p['category'])}" for p in imp.pending]
    todo = []
    if imp.sign_in:
        todo.append(txt("sign_in", names=", ".join(q(n) for n in sorted(imp.sign_in))))
        todo.append("[[IMG:sign_in]]")
    if imp.copies:
        todo.append(txt("review_copies"))
    if todo:
        lines += ["", txt("todo")] + todo
    lines += ["", txt("closing")]
    return txt("subject"), "\n".join(lines)


def user_email(api: QuickApi, user: str, cache: dict) -> str:
    if user not in cache:
        m = USER_ARN.match(user)
        email = ""
        if m:
            try:
                email = api.describe_user(m["name"], m["ns"]).get("Email") or ""
            except ClientError as exc:
                LOG.warning("Could not read the email of %s: %s", user, exc)
            if not email and "@" in m["name"]:
                email = m["name"]  # Identity Center user names are usually the email address
        cache[user] = email
    return cache[user]


def write_user_messages(run_dir: Path, impacts: dict[str, UserImpact], api: QuickApi,
                        planned: bool = False, change_date: str | None = None) -> tuple[int, Path]:
    """Write the French/English messages. planned=True (dry run): announcements in the future tense, written to
    user_messages_planned.*; otherwise what --apply did, written to user_messages.*."""
    today = dt.date.today().isoformat()
    stem = "user_messages_planned" if planned else "user_messages"
    md_path, csv_path = run_dir / f"{stem}.md", run_dir / f"{stem}.csv"
    cache: dict[str, str] = {}
    used_shots: set[str] = set()
    if planned:
        header = (f"Generated on {today} from a dry run: these messages announce changes that haven't been made yet. "
                  f"They describe what `--apply` would do with the same options, if nothing changes in the "
                  f"account in between. After `--apply`, send the messages in `user_messages.md` instead, which "
                  f"describe what was actually done.")
    else:
        header = f"Generated on {today}, after `--apply`: these messages describe what was done."
    rows, md = [], ["# Messages for the impacted users", "", header, "",
                    f"One section per user, in French and in English. The same messages are in `{csv_path.name}` "
                    f"for a mail merge. Admin notes are for you, not for the user.", ""]
    for user, imp in sorted(impacts.items()):
        email = user_email(api, user, cache)
        day_fr = (change_date or DATE_PLACEHOLDER["fr"]) if planned else today
        day_en = (change_date or DATE_PLACEHOLDER["en"]) if planned else today
        subject_fr, raw_fr = _message(imp, "fr", day_fr, planned)
        subject_en, raw_en = _message(imp, "en", day_en, planned)
        text_fr, shots = render_body(raw_fr, "fr", markdown=False)
        text_en, _ = render_body(raw_en, "en", markdown=False)
        md_fr, _ = render_body(raw_fr, "fr", markdown=True)
        md_en, _ = render_body(raw_en, "en", markdown=True)
        used_shots.update(shots)
        notes = " | ".join(imp.notes)
        rows.append({"user": user, "email": email, "subject_fr": subject_fr, "body_fr": text_fr,
                     "subject_en": subject_en, "body_en": text_en, "screenshots": "; ".join(shots),
                     "admin_notes": notes})
        md += [f"## {email or user}", "", f"Quick user: `{user}`", "",
               "### Français", "", f"**Objet :** {subject_fr}", "", md_fr, "",
               "### English", "", f"**Subject:** {subject_en}", "", md_en, ""]
        if notes:
            md += ["### Admin notes", ""] + [f"- {n}" for n in imp.notes] + [""]
    # Copy the screenshots next to the messages so the Markdown file is self-contained.
    if used_shots:
        shots_dir = run_dir / "screenshots"
        shots_dir.mkdir(exist_ok=True)
        for name in sorted(used_shots):
            shutil.copy2(SCREENSHOT_DIR / name, shots_dir / name)
    elif impacts and not SCREENSHOT_DIR.is_dir():
        LOG.warning("No screenshots found in %s; the messages are written without them", SCREENSHOT_DIR)
    write_csv(csv_path, USER_MESSAGE_FIELDS, rows)
    md_path.write_text("\n".join(md))
    return len(rows), md_path


def collect_planned_impacts(groups: list[Group], usage: "Usage", access_preview: list[dict],
                            keep_duplicates: bool) -> dict[str, UserImpact]:
    """Dry run: per Quick user, what --apply is expected to do, from the scan and the pre-checks."""
    impacts: dict[str, UserImpact] = {}
    blocked_dups = {r["duplicate_connector_id"] for r in access_preview if r["status"] == "will be skipped"}

    def of(user: str) -> UserImpact:
        return impacts.setdefault(user, UserImpact(user))

    for g in groups:
        if not g.canonical:
            continue
        canon = g.canonical
        for dup in g.duplicates:
            blocked = dup.id in blocked_dups
            refs = usage.of(dup)
            any_pending = blocked or any(r.kind == "flow" and r.detail.get("blocked_by") for r in refs)
            deleted = not any_pending and not keep_duplicates
            category = "" if deleted else "access" if blocked else "kept" if keep_duplicates else "in_use"
            for user in _users_only(dup.owners()):
                of(user).connectors.append({"name": dup.name, "type": dup.type, "canonical": canon.name,
                                            "deleted": deleted, "category": category})
                if deleted:
                    of(user).sign_in.add(canon.name)
            for ref in refs:
                problems = ref.detail.get("blocked_by") if ref.kind == "flow" else None
                for user in _users_only(ref.owners):
                    imp = of(user)
                    if blocked:
                        imp.pending.append({"kind": ref.kind, "name": ref.name, "category": "access"})
                    elif problems:
                        joined = "; ".join(problems)
                        imp.pending.append({"kind": ref.kind, "name": ref.name, "category": pending_category(joined)})
                        imp.notes.append(f"{ref.kind} {ref.name} ({ref.resource_id}): {joined}")
                    else:
                        imp.switched.append((ref.kind, ref.name, canon.name))
                        imp.sign_in.add(canon.name)
                        if ref.kind == "flow" and ref.detail.get("plan") == "copy-draft":
                            imp.copies.append({"flow": ref.name, "copy": draft_copy_name(ref.name),
                                               "canonical": canon.name, "original_migrated": True})
                            imp.notes.append(f"draft copy planned for {ref.resource_id}: "
                                             + "; ".join(ref.detail.get("draft_changes") or []))
    for row in access_preview:
        if row["user"] in impacts:
            impacts[row["user"]].notes.append(
                f"no access to {row['canonical_connector_name']} today ({row['status']}): {row['reason']}")
    return {u: i for u, i in impacts.items() if i.connectors or i.switched or i.copies or i.pending}


def print_access_preview(rows: list[dict], path: Path) -> None:
    if not rows:
        return
    print("Access to the canonical connector (users of a duplicate who cannot use it today):")
    for r in rows:
        print(f"  {r['status']:15} {r['user']}  [{r['duplicate_connector_name']} -> {r['canonical_connector_name']}]"
              f"  {r['reason']}")
    print(f"  Written to {path}. Users who would still lack access after --apply are skipped: their duplicate "
          "and its agents/flows/spaces are left unchanged.\n")


def plan_report(groups: list[Group], generic: list[Connector], usage: Usage, account: str, region: str) -> dict:
    return {
        "account": account,
        "region": region,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "groups": [
            {
                "type": g.type,
                "skip_reason": g.skip_reason,
                "canonical": g.canonical.summary() if g.canonical else None,
                "connectors": [
                    {**c.summary(), "references": [r.as_dict() for r in usage.of(c)]} for c in g.connectors
                ],
            }
            for g in groups
        ],
        "generic_http_workspace_connectors": [c.summary() | {"endpoints": c.endpoints} for c in generic],
        "users": users_view(groups, usage),
        "scan_errors": usage.errors,
    }


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Consolidate duplicated AWS Quick workspace connectors (Google Workspace, Microsoft 365) "
                    "onto the IT-managed connector. Dry run unless --apply.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--profile", help="AWS CLI profile")
    p.add_argument("--region", required=True, help="Region of the AWS Quick account (identity region)")
    p.add_argument("--account-id", help="AWS account ID (default: from STS)")
    p.add_argument("--canonical", action="append", default=[], metavar="ID_OR_ARN",
                   help="The legitimate connector for its type. Repeat once per type (Gmail, Calendar, ...).")
    p.add_argument("--non-interactive", action="store_true",
                   help="Never prompt; types without --canonical are skipped")
    p.add_argument("--type-pattern", default=DEFAULT_TYPE_PATTERN,
                   help=f"Regex on connector Type selecting connectors in scope (default: {DEFAULT_TYPE_PATTERN!r})")
    p.add_argument("--apply", action="store_true", help="Make the changes. Without it the script only reports.")
    p.add_argument("--yes", action="store_true", help="Skip the confirmation prompt when using --apply")
    p.add_argument("--keep-duplicates", action="store_true",
                   help="Re-point agents/flows/spaces but do not delete the duplicate connectors")
    p.add_argument("--grant-scope", choices=("duplicate", "all", "none"), default="duplicate",
                   help="Share the canonical connector with: the duplicate's principals (default), those plus "
                        "owners of the affected agents/flows/spaces (all), or nobody (none)")
    p.add_argument("--grant-actions", type=lambda s: [a.strip() for a in s.split(",") if a.strip()],
                   help="Comma-separated permission actions to grant on the canonical connector. "
                        "Default: inferred from an existing non-owner permission")
    p.add_argument("--flow-divergence", choices=("copy-draft", "skip", "use-draft", "use-published"),
                   default="copy-draft",
                   help="When a flow's draft has unpublished edits: copy the re-pointed draft into a new flow "
                        "visible to the owners only and update the original from its published version "
                        "(copy-draft, default); leave the flow unchanged (skip); or overwrite both versions "
                        "with the draft (use-draft, publishes the edits) or the published one (use-published, "
                        "loses the edits)")
    p.add_argument("--change-date", metavar="TEXT",
                   help="Dry run only: the date of the planned change, written into the announcement messages "
                        "(for example 2026-11-20). Default: a placeholder to fill in")
    p.add_argument("--output-dir", default="runs", help="Where plan, results and backups are written")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
    if not args.verbose:
        logging.getLogger("botocore").setLevel(logging.WARNING)

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    account = args.account_id or session.client("sts").get_caller_identity()["Account"]
    client = session.client("quicksight", config=Config(retries={"mode": "adaptive", "max_attempts": 10}))
    api = QuickApi(client, account)

    run_dir = Path(args.output_dir) / dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    connectors, generic = discover_connectors(api, re.compile(args.type_pattern, re.IGNORECASE))
    usage = scan_usage(api, connectors)
    groups = build_groups(connectors)
    interactive = not args.non_interactive and sys.stdin.isatty()
    select_canonicals(groups, usage, args.canonical, interactive)

    annotate_flows(groups, usage, args.flow_divergence)
    access = AccessChecker(api)
    preview = preview_access(groups, usage, access, args, connectors)
    print_plan(groups, generic, usage)
    print_access_preview(preview, run_dir / ACCESS_ISSUES_FILE)
    sys.stdout.flush()
    report = plan_report(groups, generic, usage, account, args.region)
    report["access_preview"] = preview
    (run_dir / "plan.json").write_text(json.dumps(report, indent=2, default=str))
    LOG.info("Plan written to %s", run_dir / "plan.json")

    actionable = [g for g in groups if g.canonical and g.duplicates]
    if not args.apply:
        write_access_issues(run_dir / ACCESS_ISSUES_FILE, preview)
        flow_rows = preview_flow_issues(groups, usage)
        write_csv(run_dir / FLOWS_FILE, FLOW_ISSUE_FIELDS, flow_rows)
        if flow_rows:
            print(f"{len(flow_rows)} flow(s) need the owner's attention (skipped, or a draft copy will be "
                  f"created). See {run_dir / FLOWS_FILE}")
        planned = collect_planned_impacts(groups, usage, preview, args.keep_duplicates)
        count_messages, messages_md = write_user_messages(run_dir, planned, api, planned=True,
                                                          change_date=args.change_date)
        if count_messages:
            print(f"{count_messages} announcement message(s) in French and English, for the users this run would "
                  f"affect: {messages_md}" + ("" if args.change_date else
                                              " (pass --change-date to fill in the date of the change)"))
        print("Dry run: nothing changed. Re-run with --apply to execute this plan.")
        return 0
    if not actionable:
        print("Nothing to do.")
        return 0

    count = sum(len(g.duplicates) for g in actionable)
    action = "re-point and keep" if args.keep_duplicates else "re-point and PERMANENTLY DELETE"
    if not args.yes:
        if not sys.stdin.isatty():
            LOG.error("Refusing to apply without a terminal; pass --yes to confirm non-interactively")
            return 2
        answer = input(f"About to {action} {count} duplicate connector(s) in account {account} "
                       f"({args.region}). Type 'yes' to continue: ")
        if answer.strip().lower() != "yes":
            print("Aborted.")
            return 1

    runner = Runner(api, args, usage, connectors, run_dir / "backups", access)
    results = []
    for g in actionable:
        for dup in g.duplicates:
            results.append(runner.migrate(g.canonical, dup))
    (run_dir / "results.json").write_text(json.dumps(results, indent=2, default=str))
    # Overwrite the preview with what really happened: only users still without access are listed.
    write_access_issues(run_dir / ACCESS_ISSUES_FILE, runner.access_issues)
    write_csv(run_dir / FLOWS_FILE, FLOW_ISSUE_FIELDS, runner.flow_issues)
    impacts = collect_impacts(results, runner, actionable, usage, args.keep_duplicates)
    count_messages, messages_md = write_user_messages(run_dir, impacts, api)

    ok = sum(1 for r in results if r["ok"])
    deleted = sum(1 for r in results if r["deleted"])
    print(f"\n{ok}/{len(results)} duplicate(s) migrated, {deleted} deleted. Details: {run_dir / 'results.json'}")
    if runner.access_issues:
        users = sorted({i["user"] for i in runner.access_issues})
        print(f"{len(users)} user(s) cannot use the canonical connector; their duplicates were skipped. "
              f"See {run_dir / ACCESS_ISSUES_FILE}")
    if runner.flow_copies:
        print(f"{len(runner.flow_copies)} flow(s) had unpublished draft edits; a copy visible to the owners only "
              f"was created for each. See {run_dir / FLOWS_FILE}")
    skipped_flows = [r for r in runner.flow_issues if r["status"] == "skipped"]
    if skipped_flows:
        print(f"{len(skipped_flows)} flow(s) were left unchanged; their duplicates were kept. "
              f"See {run_dir / FLOWS_FILE}")
    print(f"{count_messages} user message(s) in French and English to send: {messages_md} "
          f"(and {USER_MESSAGES_CSV} for a mail merge).")
    print("Users must sign in to the canonical connector once (per-user OAuth) before their agents and flows "
          "can call it.")
    return 0 if ok == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
