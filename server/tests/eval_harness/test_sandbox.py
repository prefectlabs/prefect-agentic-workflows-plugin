"""Tests for the harness's requests to the sandbox workspace, with the API mocked."""

import json
import re
from collections.abc import Iterator

import httpx
import pytest
import respx
from support import API_KEY, WORKSPACE_API_URL

from evals.record import FlowState
from evals.sandbox import (
    Credentials,
    SandboxApi,
    SandboxConfigError,
    read_credentials,
    session_prefix,
)

PREFIX = "eval-abc123-"
BLOCK_PATH = "/block_types/slug/secret/block_documents/name/eval-github-token"


@pytest.fixture
def api() -> Iterator[respx.MockRouter]:
    with respx.mock(base_url=WORKSPACE_API_URL, assert_all_called=False) as router:
        yield router


@pytest.fixture
def sandbox() -> Iterator[SandboxApi]:
    client = SandboxApi(Credentials(WORKSPACE_API_URL, API_KEY))
    yield client
    client.close()


def flow(flow_id: str, name: str) -> dict[str, str]:
    return {"id": flow_id, "name": name}


def test_read_credentials_names_each_missing_variable():
    with pytest.raises(SandboxConfigError, match="PREFECT_API_URL, PREFECT_API_KEY"):
        read_credentials({})


def test_read_credentials_rejects_a_url_that_is_not_a_cloud_workspace():
    environ = {"PREFECT_API_URL": "http://127.0.0.1:4200/api", "PREFECT_API_KEY": "k"}

    with pytest.raises(SandboxConfigError, match="not a Prefect Cloud workspace"):
        read_credentials(environ)


def test_read_credentials_returns_the_url_and_key():
    environ = {"PREFECT_API_URL": f"{WORKSPACE_API_URL}/", "PREFECT_API_KEY": API_KEY}

    assert read_credentials(environ) == Credentials(WORKSPACE_API_URL, API_KEY)


def test_session_prefix_is_new_each_session():
    first, second = session_prefix(), session_prefix()

    assert re.fullmatch(r"eval-[0-9a-f]{6}-", first)
    assert first != second


def test_delete_flows_with_prefix_deletes_only_flows_whose_name_starts_with_it(
    api: respx.MockRouter, sandbox: SandboxApi
):
    # `like_` matches anywhere in the name and ignores case.
    filter_route = api.post("/flows/filter").respond(
        200,
        json=[
            flow("1", f"{PREFIX}1-digest"),
            flow("2", f"{PREFIX}2-release-notes"),
            flow("3", f"team-{PREFIX}digest"),
            flow("4", PREFIX.upper() + "digest"),
        ],
    )
    delete_route = api.delete(url__regex=r"/flows/(?P<flow_id>[^/]+)$").respond(204)

    deleted = sandbox.delete_flows_with_prefix(PREFIX)

    assert deleted == [f"{PREFIX}1-digest", f"{PREFIX}2-release-notes"]
    body = json.loads(filter_route.calls[0].request.content)
    assert body["flows"] == {"name": {"like_": PREFIX}}
    deleted_paths = [call.request.url.path for call in delete_route.calls]
    assert [path.rsplit("/", 1)[-1] for path in deleted_paths] == ["1", "2"]


def test_delete_flows_with_prefix_tries_every_flow_before_it_raises(
    api: respx.MockRouter, sandbox: SandboxApi
):
    api.post("/flows/filter").respond(
        200, json=[flow("1", f"{PREFIX}digest"), flow("2", f"{PREFIX}notes")]
    )
    api.delete("/flows/1").respond(500)
    second = api.delete("/flows/2").respond(204)

    with pytest.raises(RuntimeError, match=f"{PREFIX}digest \\(HTTP 500\\)"):
        sandbox.delete_flows_with_prefix(PREFIX)
    assert second.called


@pytest.mark.parametrize("prefix", ["", "eval-", "digest-"])
def test_delete_flows_with_prefix_refuses_a_prefix_without_a_session_id(
    sandbox: SandboxApi, prefix: str
):
    with pytest.raises(ValueError, match="Refusing"):
        sandbox.delete_flows_with_prefix(prefix)


def test_snapshot_reads_each_flow_by_its_name_without_the_prefix(
    api: respx.MockRouter, sandbox: SandboxApi
):
    plan = {"kind": "ExecutionPlan", "nodes": {}}
    schedule = {"id": "schedule-1", "name": "monday-digest", "active": False}
    api.post("/flows/filter").respond(200, json=[flow("flow-1", f"{PREFIX}digest")])
    api.get("/flows/flow-1/execution-plan").respond(
        200, json={"active_version": {"id": "version-2", "plan": plan}}
    )
    api.get("/flows/flow-1/execution-plan/versions").respond(
        200,
        json={
            "results": [
                {"id": "version-2", "created": "2026-10-08T10:00:00Z"},
                {"id": "version-1", "created": "2026-10-08T09:00:00Z"},
            ],
            "count": 2,
            "page": 1,
            "pages": 1,
        },
    )
    api.get("/flows/flow-1/execution-plan/schedules").respond(
        200, json={"schedules": [schedule]}
    )

    assert sandbox.snapshot(PREFIX) == {
        "digest": FlowState(
            id="flow-1",
            name=f"{PREFIX}digest",
            active_version_id="version-2",
            active_plan=plan,
            version_ids=["version-1", "version-2"],
            schedules=[schedule],
        )
    }


def test_snapshot_of_a_flow_with_no_active_version(
    api: respx.MockRouter, sandbox: SandboxApi
):
    api.post("/flows/filter").respond(200, json=[flow("flow-1", f"{PREFIX}digest")])
    api.get("/flows/flow-1/execution-plan").respond(200, json={"active_version": None})
    api.get("/flows/flow-1/execution-plan/versions").respond(
        200, json={"results": [], "count": 0, "page": 1, "pages": 1}
    )
    api.get("/flows/flow-1/execution-plan/schedules").respond(
        200, json={"schedules": []}
    )

    state = sandbox.snapshot(PREFIX)["digest"]

    assert state.active_version_id is None
    assert state.active_plan is None
    assert state.version_ids == []


def test_create_schedule_creates_an_inactive_schedule(
    api: respx.MockRouter, sandbox: SandboxApi
):
    route = api.post("/flows/flow-1/execution-plan/schedules").respond(
        201, json={"id": "schedule-1"}
    )

    sandbox.create_schedule(
        "flow-1", "monday-digest", {"type": "cron", "cron": "0 9 * * 1"}, {}
    )

    assert json.loads(route.calls[0].request.content)["active"] is False


def test_publish_version_saves_and_activates_the_plan(
    api: respx.MockRouter, sandbox: SandboxApi
):
    versions = "/flows/flow-1/execution-plan/versions"
    save = api.post(versions).respond(201, json={"id": "version-1"})
    activate = api.post(f"{versions}/version-1/activate").respond(200, json={})

    sandbox.publish_version("flow-1", {"kind": "ExecutionPlan"})

    assert json.loads(save.calls[0].request.content) == {
        "plan": {"kind": "ExecutionPlan"}
    }
    assert activate.called


def test_ensure_secret_block_leaves_an_existing_block(
    api: respx.MockRouter, sandbox: SandboxApi
):
    api.get(BLOCK_PATH).respond(200, json={"id": "block-1"})
    create = api.post("/block_documents/").respond(201, json={})

    sandbox.ensure_secret_block("eval-github-token")

    assert not create.called


@pytest.mark.parametrize("status_code", [201, 409])
def test_ensure_secret_block_creates_a_missing_block(
    api: respx.MockRouter, sandbox: SandboxApi, status_code: int
):
    api.get(BLOCK_PATH).respond(404, json={"detail": "Block document not found"})
    api.get("/block_types/slug/secret").respond(200, json={"id": "type-1"})
    schemas = api.post("/block_schemas/filter").respond(200, json=[{"id": "schema-1"}])
    create = api.post("/block_documents/").respond(status_code, json={})

    sandbox.ensure_secret_block("eval-github-token")

    assert json.loads(schemas.calls[0].request.content)["block_schemas"] == {
        "block_type_id": {"any_": ["type-1"]}
    }
    body = json.loads(create.calls[0].request.content)
    assert body["name"] == "eval-github-token"
    assert body["block_type_id"] == "type-1"
    assert body["block_schema_id"] == "schema-1"
    assert set(body["data"]) == {"value"}


def test_an_unexpected_error_response_raises(
    api: respx.MockRouter, sandbox: SandboxApi
):
    api.post("/flows/filter").respond(500, json={"detail": "boom"})

    with pytest.raises(httpx.HTTPStatusError):
        sandbox.snapshot(PREFIX)
