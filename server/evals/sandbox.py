"""Requests the harness sends to the Prefect Cloud sandbox workspace.

The harness uses these to seed the state a scenario starts from, to record
the state of a run's flows after the conversation, and to delete the flows
of a test session. Every flow the harness creates or deletes has a name that
starts with the session's prefix, so other flows in the workspace are never
changed.

Deleting a flow also deletes its execution plan, its plan versions, and its
schedules.
"""

import os
import secrets
from dataclasses import dataclass, field
from typing import Any

import httpx

from evals.record import FlowState
from prefect_agentic_workflows_mcp.workspace_api import is_cloud_workspace_api_url

API_URL_VARIABLE = "PREFECT_API_URL"
API_KEY_VARIABLE = "PREFECT_API_KEY"
MISSING_CREDENTIALS_MESSAGE = (
    "The evaluations run against a Prefect Cloud sandbox workspace. Set "
    "PREFECT_API_URL to the sandbox's workspace API URL and PREFECT_API_KEY to "
    "an API key for it, then run them again. Missing: {missing}."
)
NOT_CLOUD_MESSAGE = (
    "PREFECT_API_URL is {api_url}, which is not a Prefect Cloud workspace API "
    "URL. Set it to the sandbox's URL, which looks like "
    "https://api.prefect.cloud/api/accounts/<id>/workspaces/<id>."
)
SECRET_BLOCK_TYPE_SLUG = "secret"
PLACEHOLDER_SECRET_VALUE = "Bearer eval-placeholder-token"
PAGE_SIZE = 200
TIMEOUT_SECONDS = 30.0


class SandboxConfigError(Exception):
    """The environment doesn't name a Prefect Cloud workspace and API key."""


@dataclass(frozen=True)
class Credentials:
    api_url: str
    api_key: str = field(repr=False)


def read_credentials(environ: dict[str, str] | None = None) -> Credentials:
    """Return the sandbox's API URL and key from the environment.

    Raises `SandboxConfigError` with a message that says what to set when
    either one is missing, or when the URL isn't a Cloud workspace URL.
    """
    environ = dict(os.environ) if environ is None else environ
    api_url = environ.get(API_URL_VARIABLE, "")
    api_key = environ.get(API_KEY_VARIABLE, "")
    missing = [
        name
        for name, value in [(API_URL_VARIABLE, api_url), (API_KEY_VARIABLE, api_key)]
        if not value
    ]
    if missing:
        raise SandboxConfigError(
            MISSING_CREDENTIALS_MESSAGE.format(missing=", ".join(missing))
        )
    if not is_cloud_workspace_api_url(api_url):
        raise SandboxConfigError(NOT_CLOUD_MESSAGE.format(api_url=api_url))
    return Credentials(api_url.rstrip("/"), api_key)


def session_prefix() -> str:
    """Return a new flow-name prefix for one test session, such as `eval-3f9a1c-`."""
    return f"eval-{secrets.token_hex(3)}-"


class SandboxApi:
    """Sends requests to the sandbox workspace with `httpx`.

    Raises `httpx.HTTPStatusError` for an error response that a method
    doesn't expect.
    """

    def __init__(self, credentials: Credentials, client: httpx.Client | None = None):
        self.credentials = credentials
        self.client = client or httpx.Client(
            base_url=credentials.api_url,
            headers={"Authorization": f"Bearer {credentials.api_key}"},
            timeout=TIMEOUT_SECONDS,
        )

    def close(self) -> None:
        self.client.close()

    def send(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        response = self.client.request(method, path, **kwargs)
        response.raise_for_status()
        return response

    # Seeding a scenario's starting state.

    def create_flow(self, name: str) -> dict[str, Any]:
        """Create a flow, or return the existing flow with this name."""
        return self.send("POST", "/flows/", json={"name": name, "tags": []}).json()

    def publish_version(self, flow_id: str, plan: dict[str, Any]) -> dict[str, Any]:
        """Save a plan version of the flow and make it the active version."""
        versions = f"/flows/{flow_id}/execution-plan/versions"
        version = self.send("POST", versions, json={"plan": plan}).json()
        self.send("POST", f"{versions}/{version['id']}/activate")
        return version

    def create_schedule(
        self,
        flow_id: str,
        name: str,
        schedule: dict[str, Any],
        parameters: dict[str, Any],
    ) -> dict[str, Any]:
        """Create an inactive schedule, so it never starts a run."""
        return self.send(
            "POST",
            f"/flows/{flow_id}/execution-plan/schedules",
            json={
                "name": name,
                "active": False,
                "schedule": schedule,
                "parameters": parameters,
            },
        ).json()

    def ensure_secret_block(self, name: str) -> None:
        """Create a Secret block with a placeholder value unless one has this name.

        The block is shared by every session, and the harness never deletes it.
        """
        found = self.client.get(
            f"/block_types/slug/{SECRET_BLOCK_TYPE_SLUG}/block_documents/name/{name}",
            params={"include_secrets": False},
        )
        if found.status_code != 404:
            found.raise_for_status()
            return
        block_type = self.send(
            "GET", f"/block_types/slug/{SECRET_BLOCK_TYPE_SLUG}"
        ).json()
        schemas = self.send(
            "POST",
            "/block_schemas/filter",
            json={
                "block_schemas": {"block_type_id": {"any_": [block_type["id"]]}},
                "limit": 1,
            },
        ).json()
        created = self.client.post(
            "/block_documents/",
            json={
                "name": name,
                "data": {"value": PLACEHOLDER_SECRET_VALUE},
                "block_schema_id": schemas[0]["id"],
                "block_type_id": block_type["id"],
            },
        )
        # Another session created the block between the read and this request.
        if created.status_code != 409:
            created.raise_for_status()

    # Reading and deleting the flows of a prefix.

    def flows_with_prefix(self, prefix: str) -> list[dict[str, Any]]:
        """Return every flow whose name starts with `prefix`.

        Cloud's `like_` filter matches the text anywhere in the name and
        ignores case, so this checks the start of each name too.
        """
        flows: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = self.send(
                "POST",
                "/flows/filter",
                json={
                    "flows": {"name": {"like_": prefix}},
                    "sort": "NAME_ASC",
                    "limit": PAGE_SIZE,
                    "offset": offset,
                },
            ).json()
            flows.extend(flow for flow in page if flow["name"].startswith(prefix))
            if len(page) < PAGE_SIZE:
                return flows
            offset += PAGE_SIZE

    def delete_flows_with_prefix(self, prefix: str) -> list[str]:
        """Delete every flow whose name starts with `prefix`, and return their names.

        Raises `ValueError` for a prefix that isn't an evaluation prefix, so a
        mistake can't delete other flows in the workspace. Tries every flow
        before it raises `RuntimeError` for the ones it couldn't delete.
        """
        if not prefix.startswith("eval-") or len(prefix) <= len("eval-"):
            raise ValueError(f"Refusing to delete flows with the prefix {prefix!r}.")
        deleted: list[str] = []
        failed: list[str] = []
        for flow in self.flows_with_prefix(prefix):
            response = self.client.delete(f"/flows/{flow['id']}")
            if response.is_success or response.status_code == 404:
                deleted.append(flow["name"])
            else:
                failed.append(f"{flow['name']} (HTTP {response.status_code})")
        if failed:
            raise RuntimeError(f"Could not delete these flows: {', '.join(failed)}")
        return deleted

    def flow_state(self, flow: dict[str, Any]) -> FlowState:
        """Read a flow's active version, plan versions, and schedules."""
        plan_path = f"/flows/{flow['id']}/execution-plan"
        active = self.send("GET", plan_path).json().get("active_version") or {}
        versions: list[dict[str, Any]] = []
        page = 1
        while True:
            body = self.send(
                "GET", f"{plan_path}/versions", params={"page": page}
            ).json()
            versions.extend(body.get("results") or [])
            if page >= int(body.get("pages") or 1):
                break
            page += 1
        versions.sort(key=lambda version: str(version.get("created", "")))
        schedules = self.send("GET", f"{plan_path}/schedules").json()
        return FlowState(
            id=str(flow["id"]),
            name=flow["name"],
            active_version_id=str(active["id"]) if active.get("id") else None,
            active_plan=active.get("plan"),
            version_ids=[str(version["id"]) for version in versions],
            schedules=list(schedules.get("schedules") or []),
        )

    def snapshot(self, prefix: str) -> dict[str, FlowState]:
        """Return the state of each flow whose name starts with `prefix`.

        The keys are the flow names without the prefix.
        """
        return {
            flow["name"].removeprefix(prefix): self.flow_state(flow)
            for flow in self.flows_with_prefix(prefix)
        }
