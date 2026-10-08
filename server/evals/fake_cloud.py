"""An in-process fake of the Prefect Cloud workspace API for evaluations.

`FakeCloud` answers the requests the MCP server sends, and keeps state between
them: flows, plan versions and which one is active, schedules, Secret blocks,
deployments, and runs. `FakeCloud.handle` takes an `httpx.Request` and
returns an `httpx.Response`, so tests can route requests to it with `respx`,
and the runner can serve it over HTTP with `evals.runner.serve`.

`validate_plan` checks the document shape against a copy of the JSON Schema
that Cloud serves, then only the rules that a fake run depends on: edges and
plan outputs point at nodes and ports that exist, there is no cycle, and a
human-input node with more than one response output has a `decision` enum
that lists those outputs. It doesn't check anything else Cloud checks. The
scenarios check the rest of the plan after the run with `evals.assertions`.

A run moves one node forward each time it is read. Agent, deployment, and
timer nodes complete with their first declared output unless a `NodeScript`
says otherwise. A human-input node waits until a response is submitted, then
selects the output its `decision` names.

`FakeCloud.lose_response` makes the fake do what a request asks and then
answer 504, as when a gateway times out after Cloud acted on the request.

The copies of the schema are in `evals/schemas/`. They were copied from
`GET /execution-plans/schema` on 2026-09-28, when Cloud's current version was
0.1 and its newest was 0.2.
"""

import hashlib
import json
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import httpx
import jsonschema

from evals import graph

SCHEMAS_DIR = Path(__file__).parent / "schemas"
CURRENT_SCHEMA_VERSION = "0.1"
SUPPORTED_SCHEMA_VERSIONS = ["0.1", "0.2"]
FAKE_USER = {"type": "USER", "display_value": "eval-user"}

Json = Any


def load_schema(version: str) -> dict[str, Any]:
    return json.loads((SCHEMAS_DIR / f"{version}.json").read_text())


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id() -> str:
    return str(uuid4())


def json_response(status_code: int, body: Json, **headers: str) -> httpx.Response:
    return httpx.Response(status_code, json=body, headers=headers)


def error(status_code: int, detail: Json) -> httpx.Response:
    return json_response(status_code, {"detail": detail})


def example_value(schema: Any) -> Json:
    """Return a value that matches a simple JSON Schema.

    Agent and deployment nodes in a fake run produce this value when the
    scenario doesn't script one.
    """
    if not isinstance(schema, dict):
        return {}
    if "const" in schema:
        return schema["const"]
    if schema.get("enum"):
        return schema["enum"][0]
    if schema.get("anyOf") or schema.get("oneOf"):
        return example_value((schema.get("anyOf") or schema["oneOf"])[0])
    kind = schema.get("type")
    if isinstance(kind, list):
        kind = next((item for item in kind if item != "null"), "null")
    if kind == "object" or "properties" in schema:
        properties = schema.get("properties") or {}
        return {name: example_value(sub) for name, sub in properties.items()}
    if kind == "boolean":
        return True
    if kind == "null":
        return None
    return {"array": [], "string": "example", "integer": 1, "number": 1}.get(kind, {})


def plan_output_sources(field_spec: dict[str, Any]) -> list[dict[str, Any]]:
    source = field_spec.get("source")
    if not isinstance(source, dict):
        return []
    if source.get("type") == "one_of":
        return [ref for ref in source.get("one_of") or [] if isinstance(ref, dict)]
    return [source]


def reference_errors(plan: graph.Plan) -> list[str]:
    """Return a message for each node port that an edge or a plan output names but
    no node has."""
    ports = [(graph.edge_target(edge), "input") for edge in graph.edges(plan)]
    ports += [
        (graph.edge_source(edge), "output")
        for edge in graph.edges(plan)
        if graph.edge_source(edge).get("type") == "node_output"
    ]
    for output in (plan.get("outputs") or {}).values():
        for field_spec in (output.get("fields") or {}).values():
            ports += [(ref, "output") for ref in plan_output_sources(field_spec)]
    nodes = graph.nodes(plan)
    return [
        f"No node has the {side} {ref.get('node')}.{ref.get(side)}."
        for ref, side in ports
        if ref.get(side)
        not in (nodes.get(str(ref.get("node"))) or {}).get(f"{side}s", {})
    ]


def decision_errors(plan: graph.Plan) -> list[str]:
    """Return a message for each approval whose `decision` enum can't select outputs."""
    errors = []
    for node_id, node in graph.nodes(plan).items():
        outputs = graph.response_outputs(node)
        form = (node.get("human_input") or {}).get("form_schema") or {}
        decision = (form.get("properties") or {}).get("decision") or {}
        if node.get("kind") != "HumanInputNode" or len(outputs) < 2:
            continue
        if set(decision.get("enum") or []) != set(outputs):
            errors.append(f"The `decision` enum of {node_id!r} must be {outputs!r}.")
    return errors


def validate(plan: Json) -> list[dict[str, Any]]:
    """Return the errors `POST /execution-plans/validate` reports for a plan."""
    version = plan.get("schema_version") if isinstance(plan, dict) else None
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        version = CURRENT_SCHEMA_VERSION
    validator = jsonschema.Draft202012Validator(load_schema(version))
    shape = sorted(validator.iter_errors(plan), key=lambda e: list(e.absolute_path))
    if shape:
        return [
            {
                "code": str(problem.validator),
                "phase": "document_shape",
                "path": list(problem.absolute_path),
                "message": problem.message,
            }
            for problem in shape
        ]
    messages = [*reference_errors(plan), *decision_errors(plan)]
    if cycle := graph.find_cycle(plan):
        messages.append(f"The edges between nodes {cycle!r} form a cycle.")
    return [
        {"code": "invalid_plan", "phase": "semantic", "path": [], "message": message}
        for message in messages
    ]


@dataclass
class NodeScript:
    """How one node behaves in a fake run.

    `output` is the output an agent, deployment, or timer node selects, and
    defaults to the node's first output. `value` is the value it produces, and
    defaults to a value built from the output's schema. `fail` makes the node
    fail with that message. `expire` makes a human-input node's deadline pass
    the first time the run moves forward after the node starts waiting.
    """

    output: str | None = None
    value: Json = None
    fail: str | None = None
    expire: bool = False


@dataclass
class NodeState:
    status: str = "pending"
    activation_id: str | None = None
    output: str | None = None
    value: Json = None
    failure: dict[str, Any] | None = None


@dataclass
class FakeRun:
    """One run of a plan version, and the state of each of its nodes."""

    id: str
    flow_id: str
    version: dict[str, Any]
    parameters: dict[str, Any]
    idempotency_key: str | None
    scripts: dict[str, NodeScript]
    nodes: dict[str, NodeState] = field(default_factory=dict)
    responses: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.nodes = {node_id: NodeState() for node_id in graph.nodes(self.plan)}

    @property
    def plan(self) -> graph.Plan:
        return self.version["plan"]

    def flow_run(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "flow_id": self.flow_id,
            "parameters": self.parameters,
            "idempotency_key": self.idempotency_key,
            "state_type": "RUNNING",
        }

    def source_state(self, source: dict[str, Any]) -> str:
        """Return whether an edge source has `produced`, is `waiting`, or is `dead`."""
        if source.get("type") == "plan_input":
            name = source.get("input")
            plan_input = (self.plan.get("inputs") or {}).get(name) or {}
            if name in self.parameters or "default" in plan_input.get("schema", {}):
                return "produced"
            return "dead"
        state = self.nodes[str(source.get("node"))]
        if state.status == "completed":
            return "produced" if state.output == source.get("output") else "dead"
        return "dead" if state.status in ("skipped", "failed") else "waiting"

    def readiness(self, node_id: str) -> str:
        """Return `ready`, `waiting`, or `skip` for a pending node.

        An input fed by several edges has arrived when any of them produced a
        value, and waits while any of them can still produce one.
        """
        ports: dict[str, set[str]] = {}
        for edge in graph.edges(self.plan):
            target = graph.edge_target(edge)
            if target.get("node") == node_id:
                ports.setdefault(str(target.get("input")), set()).add(
                    self.source_state(graph.edge_source(edge))
                )
        if any("produced" not in s and "waiting" in s for s in ports.values()):
            return "waiting"
        if any(s == {"dead"} for s in ports.values()):
            return "skip"
        return "ready"

    def script(self, node_id: str) -> NodeScript:
        """Return the node's script by its ID, else by its kind, else the default."""
        kind = str(graph.nodes(self.plan)[node_id].get("kind"))
        return self.scripts.get(node_id) or self.scripts.get(kind) or NodeScript()

    def advance(self) -> None:
        """Move the run forward by one node, as a read of the run does."""
        order, _ = graph.topological_order(self.plan)
        node_map = graph.nodes(self.plan)
        for node_id in order:
            state = self.nodes[node_id]
            if state.status == "suspended" and self.script(node_id).expire:
                deadline = (node_map[node_id].get("human_input") or {}).get("deadline")
                on_expiry = (deadline or {}).get("on_expiry")
                if on_expiry:
                    state.status = "completed"
                    state.output = on_expiry.get("output")
                    state.value = on_expiry.get("value")
                else:
                    state.status = "failed"
                    state.failure = {"code": "human_input_expired"}
                return
        for node_id in order:
            state = self.nodes[node_id]
            if state.status != "pending":
                continue
            readiness = self.readiness(node_id)
            if readiness == "skip":
                state.status = "skipped"
            elif readiness == "ready":
                self.start_node(node_id, node_map[node_id])
                return

    def start_node(self, node_id: str, node: dict[str, Any]) -> None:
        state = self.nodes[node_id]
        script = self.script(node_id)
        state.activation_id = new_id()
        if node.get("kind") == "HumanInputNode":
            state.status = "suspended"
        elif script.fail is not None:
            state.status = "failed"
            state.failure = {"code": "scripted_failure", "message": script.fail}
        else:
            outputs = node.get("outputs") or {}
            state.status = "completed"
            state.output = script.output or next(iter(outputs), None)
            state.value = (
                script.value
                if script.value is not None
                else example_value((outputs.get(state.output) or {}).get("schema"))
            )

    def answer(self, activation_id: str, response: dict[str, Any]) -> httpx.Response:
        for node_id, state in self.nodes.items():
            if state.activation_id != activation_id:
                continue
            if state.status != "suspended":
                return error(409, "This form is no longer waiting for an answer.")
            node = graph.nodes(self.plan)[node_id]
            form = (node.get("human_input") or {}).get("form_schema") or {}
            try:
                jsonschema.validate(response, form)
            except jsonschema.ValidationError as exc:
                return error(422, f"The response doesn't match the form: {exc.message}")
            outputs = graph.response_outputs(node)
            state.status = "completed"
            state.output = str(
                response.get("decision") if len(outputs) > 1 else outputs[0]
            )
            state.value = response
            self.responses.append(
                {"node": node_id, "activation_id": activation_id, "response": response}
            )
            return json_response(201, {"id": new_id(), "activation_id": activation_id})
        return error(404, "Activation not found.")

    def status(self) -> str:
        statuses = [state.status for state in self.nodes.values()]
        if "failed" in statuses:
            return "failed"
        if all(status in ("completed", "skipped") for status in statuses):
            return "completed"
        if "suspended" in statuses and not any(
            state.status == "pending" and self.readiness(node_id) == "ready"
            for node_id, state in self.nodes.items()
        ):
            return "awaiting_external_progress"
        return "running"

    def plan_output(self, name: str) -> tuple[str, Json]:
        """Return a plan output's status and, when it is available, its value."""
        output = (self.plan.get("outputs") or {}).get(name) or {}
        value: dict[str, Any] = {}
        for field_name, field_spec in (output.get("fields") or {}).items():
            for ref in plan_output_sources(field_spec):
                state = self.source_state(ref)
                if state == "waiting":
                    return "waiting", None
                if state == "produced":
                    value[field_name] = self.nodes[str(ref.get("node"))].value
                    break
        return ("available", value) if value else ("skipped", None)

    def observation(self) -> dict[str, Any]:
        node_map = graph.nodes(self.plan)
        nodes = []
        for node_id, state in self.nodes.items():
            node = node_map[node_id]
            body: dict[str, Any] = {
                "node": node_id,
                "kind": node.get("kind"),
                "status": state.status,
                "activation_id": state.activation_id,
                "outputs": [
                    {"output": name, "status": output_status(state, name)}
                    for name in graph.output_names(node)
                ],
            }
            if state.status == "suspended":
                deadline = datetime.now(timezone.utc) + timedelta(days=3)
                body["wait"] = {
                    "kind": "human_input",
                    "activation_id": state.activation_id,
                    "form_schema": (node.get("human_input") or {}).get("form_schema"),
                    "deadline_at": deadline.isoformat(),
                }
            if state.failure is not None:
                body["failure"] = state.failure
            nodes.append(body)
        return {
            "snapshot": {
                "flow_run_id": self.id,
                "execution_plan_version_id": self.version["id"],
            },
            "status": self.status(),
            "nodes": nodes,
            "outputs": [
                {"output": name, "status": self.plan_output(name)[0]}
                for name in self.plan.get("outputs") or {}
            ],
            "diagnostics": [],
        }


def output_status(state: NodeState, name: str) -> str:
    if state.status == "completed":
        return "available" if state.output == name else "skipped"
    return "skipped" if state.status in ("skipped", "failed") else "pending"


def unavailable(status: str) -> httpx.Response:
    """Return the 409 Cloud answers for an output that isn't available."""
    if status == "skipped":
        body = {"output_status": "skipped", "reason": "not_produced"}
        return json_response(409, {"detail": "The output was not produced.", **body})
    body = {"output_status": "waiting", "reason": "not_ready"}
    return json_response(
        409,
        {"detail": "The output is not available yet.", **body},
        **{"Retry-After": "2"},
    )


@dataclass
class LostResponse:
    """Requests whose responses the fake loses after it has acted on them."""

    method: str
    pattern: re.Pattern[str]
    remaining: int


PLAN = r"/flows/(?P<flow_id>[^/]+)/execution-plan"
VERSION = rf"{PLAN}/versions/(?P<version_id>[^/]+)"
RUN = r"/flow_runs/(?P<run_id>[^/]+)/execution-plan"
ACTIVATION = rf"{RUN}/activations/(?P<activation_id>[^/]+)"


class FakeCloud:
    """A fake Prefect Cloud workspace API that keeps state between requests.

    `api_url` is the workspace API URL the server sends requests to. Only its
    path matters, so the same fake works behind `respx` and behind a local
    HTTP server. `scripts` sets how nodes behave in every run. A key is a
    node ID, or a node kind such as `HumanInputNode` for every node of that
    kind that has no script of its own. Use a kind when the agent picks the
    node IDs.
    """

    def __init__(
        self, api_url: str, *, scripts: dict[str, NodeScript] | None = None
    ) -> None:
        self.base_path = urlsplit(api_url).path.rstrip("/")
        self.scripts = scripts or {}
        self.flows: dict[str, dict[str, Any]] = {}
        self.versions: dict[str, list[dict[str, Any]]] = {}
        self.active: dict[str, dict[str, Any]] = {}
        self.schedules: dict[str, dict[str, dict[str, Any]]] = {}
        self.secret_blocks: list[dict[str, Any]] = []
        self.deployments: list[dict[str, Any]] = []
        self.runs: dict[str, FakeRun] = {}
        self.lost_responses: list[LostResponse] = []
        self._lock = threading.Lock()
        routes = [
            ("GET", r"/execution-plans/schema", self.get_schema),
            ("POST", r"/execution-plans/validate", self.validate_plan),
            ("GET", r"/flows/name/(?P<name>[^/]+)", self.read_flow_by_name),
            ("POST", r"/flows/", self.create_flow),
            ("POST", r"/flows/filter", self.filter_flows),
            ("GET", r"/flows/(?P<flow_id>[^/]+)", self.read_flow),
            ("GET", PLAN, self.read_active),
            ("GET", f"{PLAN}/versions", self.list_versions),
            ("POST", f"{PLAN}/versions", self.create_version),
            ("GET", VERSION, self.read_version),
            ("POST", f"{VERSION}/activate", self.activate_version),
            ("POST", f"{PLAN}/runs", self.start_run),
            ("GET", f"{PLAN}/schedules", self.list_schedules),
            ("GET", rf"{PLAN}/schedules/(?P<schedule_id>[^/]+)", self.read_schedule),
            ("GET", RUN, self.read_run),
            ("GET", rf"{RUN}/outputs/(?P<name>[^/]+)", self.read_plan_output),
            ("GET", rf"{ACTIVATION}/outputs/(?P<name>[^/]+)", self.read_node_output),
            ("POST", f"{ACTIVATION}/human-input/responses", self.submit_human_input),
            ("POST", r"/block_documents/filter", self.filter_block_documents),
            ("POST", r"/deployments/filter", self.filter_deployments),
            ("GET", r"/deployments/(?P<deployment_id>[^/]+)", self.read_deployment),
        ]
        self.routes = [
            (method, re.compile(f"^{pattern}$"), handler)
            for method, pattern, handler in routes
        ]

    # State that a scenario sets up before the agent starts.

    def add_flow(self, name: str, tags: list[str] | None = None) -> dict[str, Any]:
        flow = {"id": new_id(), "name": name, "tags": tags or [], "created": now()}
        self.flows[flow["id"]] = flow
        return flow

    def add_version(
        self, flow_id: str, plan: graph.Plan, *, activate: bool = True
    ) -> dict[str, Any]:
        canonical = json.dumps(plan, sort_keys=True).encode()
        version = {
            "id": new_id(),
            "flow_id": flow_id,
            "schema_version": plan.get("schema_version"),
            "semantic_hash": hashlib.sha256(canonical).hexdigest(),
            "created": now(),
            "created_by": FAKE_USER,
            "plan": plan,
        }
        self.versions.setdefault(flow_id, []).append(version)
        if activate:
            self.active[flow_id] = {**version, "activated": now()}
        return version

    def add_schedule(
        self,
        flow_id: str,
        name: str,
        schedule: dict[str, Any],
        *,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body = {
            "id": new_id(),
            "flow_id": flow_id,
            "name": name,
            "active": True,
            "schedule": schedule,
            "parameters": parameters or {},
            "execution_plan_version_selection": "active_at_scheduled_time",
        }
        self.schedules.setdefault(flow_id, {})[body["id"]] = body
        return body

    def add_secret_block(self, name: str) -> str:
        block_id = new_id()
        self.secret_blocks.append(
            {"id": block_id, "name": name, "block_type": {"slug": "secret"}}
        )
        return block_id

    def add_deployment(
        self, flow_id: str, name: str, *, description: str | None = None
    ) -> dict[str, Any]:
        deployment = {
            "id": new_id(),
            "flow_id": flow_id,
            "name": name,
            "description": description,
        }
        self.deployments.append(deployment)
        return deployment

    def lose_response(self, method: str, path: str, *, times: int = 1) -> None:
        """Lose the response to the next `times` requests that match and succeed.

        `path` is a regular expression for the workspace-relative path, such
        as `/flows/[^/]+/execution-plan/runs`. The fake still does what each
        request asks, then answers 504 with a plain-text body, the way a
        gateway does when it gives up waiting. The client can't tell whether
        the request took effect. A request the fake rejects keeps its error
        and doesn't use up a lost response.
        """
        self.lost_responses.append(LostResponse(method, re.compile(f"^{path}$"), times))

    def take_lost_response(self, method: str, path: str) -> bool:
        for lost in self.lost_responses:
            if lost.remaining and lost.method == method and lost.pattern.match(path):
                lost.remaining -= 1
                return True
        return False

    # Request handling.

    def handle(self, request: httpx.Request) -> httpx.Response:
        """Answer one request to the workspace API."""
        path = unquote(request.url.path).removeprefix(self.base_path) or "/"
        body = json.loads(request.content) if request.content else {}
        with self._lock:
            for method, pattern, handler in self.routes:
                match = pattern.match(path)
                if method != request.method or not match:
                    continue
                if (
                    "flow_id" in match.groupdict()
                    and match["flow_id"] not in self.flows
                ):
                    return error(404, "Flow not found.")
                if "run_id" in match.groupdict() and match["run_id"] not in self.runs:
                    return error(404, "Flow run not found.")
                response = handler(
                    body=body or {},
                    params=dict(request.url.params),
                    **match.groupdict(),
                )
                if response.is_success and self.take_lost_response(method, path):
                    return httpx.Response(504, text="upstream request timeout")
                return response
        return error(
            404, f"The fake Cloud API has no route for {request.method} {path}."
        )

    def get_schema(self, *, params: dict[str, str], **_: Any) -> httpx.Response:
        version = params.get("version", CURRENT_SCHEMA_VERSION)
        if version not in SUPPORTED_SCHEMA_VERSIONS:
            return error(422, f"Schema version {version!r} is not supported.")
        return json_response(
            200,
            {
                "schema_version": version,
                "schema": load_schema(version),
                "supported_schema_versions": SUPPORTED_SCHEMA_VERSIONS,
                "current_schema_version": CURRENT_SCHEMA_VERSION,
                "is_current": version == CURRENT_SCHEMA_VERSION,
                "is_deprecated": False,
                "document_shape_only": True,
            },
        )

    def validate_plan(self, *, body: Json, **_: Any) -> httpx.Response:
        errors = validate(body.get("plan"))
        return json_response(200, {"valid": not errors, "errors": errors})

    def read_flow_by_name(self, *, name: str, **_: Any) -> httpx.Response:
        for flow in self.flows.values():
            if flow["name"] == name:
                return json_response(200, flow)
        return error(404, "Flow not found.")

    def read_flow(self, *, flow_id: str, **_: Any) -> httpx.Response:
        flow = self.flows.get(flow_id)
        return json_response(200, flow) if flow else error(404, "Flow not found.")

    def create_flow(self, *, body: Json, **_: Any) -> httpx.Response:
        return json_response(201, self.add_flow(body["name"], body.get("tags")))

    def filter_flows(self, *, body: Json, **_: Any) -> httpx.Response:
        wanted = ((body.get("flows") or {}).get("id") or {}).get("any_")
        flows = [f for f in self.flows.values() if wanted is None or f["id"] in wanted]
        return json_response(200, flows)

    def read_active(self, *, flow_id: str, **_: Any) -> httpx.Response:
        return json_response(200, {"active_version": self.active.get(flow_id)})

    def list_versions(self, *, flow_id: str, **_: Any) -> httpx.Response:
        versions = self.versions.get(flow_id, [])
        summaries = [
            {key: value for key, value in version.items() if key != "plan"}
            for version in reversed(versions)
        ]
        return json_response(
            200, {"results": summaries, "count": len(versions), "page": 1, "pages": 1}
        )

    def create_version(self, *, flow_id: str, body: Json, **_: Any) -> httpx.Response:
        return json_response(
            201, self.add_version(flow_id, body["plan"], activate=False)
        )

    def find_version(self, flow_id: str, version_id: str) -> dict[str, Any] | None:
        versions = self.versions.get(flow_id, [])
        return next((v for v in versions if v["id"] == version_id), None)

    def read_version(
        self, *, flow_id: str, version_id: str, **_: Any
    ) -> httpx.Response:
        version = self.find_version(flow_id, version_id)
        if version is None:
            return error(404, "Execution plan version not found.")
        return json_response(200, version)

    def activate_version(
        self, *, flow_id: str, version_id: str, **_: Any
    ) -> httpx.Response:
        version = self.find_version(flow_id, version_id)
        if version is None:
            return error(404, "Execution plan version not found.")
        self.active[flow_id] = {**version, "activated": now()}
        return json_response(200, {"active_version": self.active[flow_id]})

    def start_run(self, *, flow_id: str, body: Json, **_: Any) -> httpx.Response:
        active = self.active.get(flow_id)
        if active is None:
            return error(404, "Active execution plan version not found.")
        key = body.get("idempotency_key")
        for run in self.runs.values():
            if key is not None and (run.flow_id, run.idempotency_key) == (flow_id, key):
                return json_response(200, run.flow_run())
        run = FakeRun(
            id=new_id(),
            flow_id=flow_id,
            version=active,
            parameters=body.get("parameters") or {},
            idempotency_key=key,
            scripts=self.scripts,
        )
        self.runs[run.id] = run
        return json_response(201, run.flow_run())

    def list_schedules(self, *, flow_id: str, **_: Any) -> httpx.Response:
        schedules = list(self.schedules.get(flow_id, {}).values())
        return json_response(200, {"schedules": schedules})

    def read_schedule(
        self, *, flow_id: str, schedule_id: str, **_: Any
    ) -> httpx.Response:
        schedule = self.schedules.get(flow_id, {}).get(schedule_id)
        if schedule is None:
            return error(404, "Schedule not found.")
        return json_response(200, schedule)

    def read_run(self, *, run_id: str, **_: Any) -> httpx.Response:
        run = self.runs[run_id]
        run.advance()
        return json_response(200, run.observation())

    def read_plan_output(self, *, run_id: str, name: str, **_: Any) -> httpx.Response:
        run = self.runs[run_id]
        if name not in (run.plan.get("outputs") or {}):
            return error(404, "Output not found.")
        status, value = run.plan_output(name)
        if status == "available" and run.status() == "completed":
            return json_response(200, value)
        return unavailable("skipped" if status == "skipped" else "waiting")

    def read_node_output(
        self, *, run_id: str, activation_id: str, name: str, **_: Any
    ) -> httpx.Response:
        states = self.runs[run_id].nodes.values()
        state = next((s for s in states if s.activation_id == activation_id), None)
        if state is None:
            return error(404, "Activation not found.")
        if output_status(state, name) == "available":
            return json_response(200, state.value)
        return unavailable(output_status(state, name))

    def submit_human_input(
        self, *, run_id: str, activation_id: str, body: Json, **_: Any
    ) -> httpx.Response:
        return self.runs[run_id].answer(activation_id, body.get("response") or {})

    def filter_block_documents(self, **_: Any) -> httpx.Response:
        return json_response(200, sorted(self.secret_blocks, key=lambda b: b["name"]))

    def filter_deployments(self, **_: Any) -> httpx.Response:
        return json_response(200, sorted(self.deployments, key=lambda d: d["name"]))

    def read_deployment(self, *, deployment_id: str, **_: Any) -> httpx.Response:
        for deployment in self.deployments:
            if deployment["id"] == deployment_id:
                return json_response(200, deployment)
        return error(404, "Deployment not found.")
