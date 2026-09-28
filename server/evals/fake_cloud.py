"""An in-process fake of the Prefect Cloud workspace API for evaluations.

`FakeCloud` answers the requests the MCP server sends, and keeps state between
them: flows, plan versions and which one is active, schedules, Secret blocks,
and runs. `FakeCloud.handle` takes an `httpx.Request` and returns an
`httpx.Response`, so tests can route requests to it with `respx`, and the
runner can serve it over HTTP with `evals.runner.serve`.

`validate_plan` checks the document shape against a copy of the JSON Schema
that Cloud serves, then runs the graph checks the skill depends on:

- edge IDs are unique
- edges point at plan inputs, nodes, and ports that exist
- plan output fields point at nodes and outputs that exist
- node-output edges don't form a cycle
- nodes other than agent nodes use `exactly_one` output selection
- a human-input node with more than one response output has a required
  `decision` property whose `enum` lists those outputs

A run moves one node forward each time it is read. Agent, deployment, and
timer nodes complete with their first declared output unless a `NodeScript`
says otherwise. A human-input node waits until a response is submitted, then
selects the output its `decision` names.

The copies of the schema are in `evals/schemas/`. They were copied from
`GET /execution-plans/schema` on 2026-09-28, when Cloud's current version was
0.1 and its newest was 0.2.
"""

import hashlib
import json
import re
import threading
from collections.abc import Callable
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
VERSIONS_PAGE_SIZE = 10
RETRY_AFTER_SECONDS = "2"
FAKE_USER = {"type": "USER", "display_value": "eval-user"}

Json = Any
Handler = Callable[..., httpx.Response]


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


def plan_error(code: str, path: list[Any], message: str) -> dict[str, Any]:
    return {"code": code, "phase": "semantic", "path": path, "message": message}


def example_value(schema: Any) -> Json:
    """Return a value that matches a simple JSON Schema.

    Agent and deployment nodes in a fake run produce this value when the
    scenario doesn't script one.
    """
    if not isinstance(schema, dict):
        return {}
    if "const" in schema:
        return schema["const"]
    if isinstance(schema.get("enum"), list) and schema["enum"]:
        return schema["enum"][0]
    for key in ("anyOf", "oneOf"):
        if isinstance(schema.get(key), list) and schema[key]:
            return example_value(schema[key][0])
    kind = schema.get("type")
    if isinstance(kind, list):
        kind = next((item for item in kind if item != "null"), "null")
    if kind == "object" or "properties" in schema:
        properties = schema.get("properties")
        if not isinstance(properties, dict):
            return {}
        return {name: example_value(sub) for name, sub in properties.items()}
    if kind == "array":
        return []
    if kind == "string":
        return "2026-09-28" if schema.get("format") == "date" else "example"
    if kind in ("integer", "number"):
        return 1
    if kind == "boolean":
        return True
    if kind == "null":
        return None
    return {}


def check_document_shape(plan: Json) -> list[dict[str, Any]]:
    if not isinstance(plan, dict):
        return [
            {
                "code": "type",
                "phase": "document_shape",
                "path": [],
                "message": "The plan must be a JSON object.",
            }
        ]
    version = plan.get("schema_version")
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        return [
            {
                "code": "unsupported_schema_version",
                "phase": "document_shape",
                "path": ["schema_version"],
                "message": (
                    f"Schema version {version!r} is not supported. Supported "
                    f"versions: {', '.join(SUPPORTED_SCHEMA_VERSIONS)}."
                ),
            }
        ]
    validator = jsonschema.Draft202012Validator(load_schema(str(version)))
    return [
        {
            "code": str(problem.validator),
            "phase": "document_shape",
            "path": list(problem.absolute_path),
            "message": problem.message,
        }
        for problem in sorted(
            validator.iter_errors(plan), key=lambda item: list(item.absolute_path)
        )
    ]


def check_edges(plan: graph.Plan) -> list[dict[str, Any]]:
    errors = []
    node_map = graph.nodes(plan)
    plan_inputs = plan.get("inputs") or {}
    seen_ids: set[str] = set()
    for index, edge in enumerate(graph.edges(plan)):
        edge_id = edge.get("id")
        if edge_id in seen_ids:
            errors.append(
                plan_error(
                    "duplicate_edge_id",
                    ["edges", index, "id"],
                    f"Edge ID {edge_id!r} is duplicated.",
                )
            )
        seen_ids.add(str(edge_id))

        source = graph.edge_source(edge)
        if source.get("type") == "plan_input":
            if source.get("input") not in plan_inputs:
                errors.append(
                    plan_error(
                        "missing_plan_input",
                        ["edges", index, "from", "input"],
                        f"Edge {edge_id!r} references missing plan input "
                        f"{source.get('input')!r}.",
                    )
                )
        elif source.get("type") == "node_output":
            source_node = node_map.get(str(source.get("node")))
            if source_node is None:
                errors.append(
                    plan_error(
                        "missing_source_node",
                        ["edges", index, "from", "node"],
                        f"Edge {edge_id!r} references missing source node "
                        f"{source.get('node')!r}.",
                    )
                )
            elif source.get("output") not in graph.output_names(source_node):
                errors.append(
                    plan_error(
                        "missing_source_output",
                        ["edges", index, "from", "output"],
                        f"Edge {edge_id!r} references missing output "
                        f"{source.get('output')!r} on source node "
                        f"{source.get('node')!r}.",
                    )
                )

        target = graph.edge_target(edge)
        target_node = node_map.get(str(target.get("node")))
        if target_node is None:
            errors.append(
                plan_error(
                    "missing_target_node",
                    ["edges", index, "to", "node"],
                    f"Edge {edge_id!r} references missing target node "
                    f"{target.get('node')!r}.",
                )
            )
        elif target.get("input") not in graph.input_names(target_node):
            errors.append(
                plan_error(
                    "missing_target_input",
                    ["edges", index, "to", "input"],
                    f"Edge {edge_id!r} references missing input "
                    f"{target.get('input')!r} on target node {target.get('node')!r}.",
                )
            )
    return errors


def plan_output_sources(field_spec: dict[str, Any]) -> list[dict[str, Any]]:
    source = field_spec.get("source")
    if not isinstance(source, dict):
        return []
    if source.get("type") == "one_of":
        options = source.get("one_of")
        return [ref for ref in options or [] if isinstance(ref, dict)]
    return [source]


def check_plan_outputs(plan: graph.Plan) -> list[dict[str, Any]]:
    errors = []
    node_map = graph.nodes(plan)
    outputs = plan.get("outputs")
    if not isinstance(outputs, dict):
        return []
    for output_name, output in outputs.items():
        for field_name, field_spec in (output.get("fields") or {}).items():
            path = ["outputs", output_name, "fields", field_name, "source"]
            for ref in plan_output_sources(field_spec):
                node = node_map.get(str(ref.get("node")))
                if node is None:
                    errors.append(
                        plan_error(
                            "missing_plan_output_source_node",
                            [*path, "node"],
                            f"Plan output field {output_name}.{field_name} "
                            f"references missing node {ref.get('node')!r}.",
                        )
                    )
                elif ref.get("output") not in graph.output_names(node):
                    errors.append(
                        plan_error(
                            "missing_plan_output_source_output",
                            [*path, "output"],
                            f"Plan output field {output_name}.{field_name} "
                            f"references missing output {ref.get('output')!r} on "
                            f"node {ref.get('node')!r}.",
                        )
                    )
    return errors


def check_cycles(plan: graph.Plan) -> list[dict[str, Any]]:
    cyclic = set(graph.find_cycle(plan))
    if not cyclic:
        return []
    for index, edge in enumerate(graph.edges(plan)):
        source = graph.edge_source(edge)
        if (
            source.get("type") == "node_output"
            and source.get("node") in cyclic
            and graph.edge_target(edge).get("node") in cyclic
        ):
            return [
                plan_error(
                    "cycle_detected",
                    ["edges", index],
                    f"Edge {edge.get('id')!r} participates in a cycle involving "
                    f"nodes {sorted(cyclic)!r}.",
                )
            ]
    return []


def check_nodes(plan: graph.Plan) -> list[dict[str, Any]]:
    errors = []
    for node_id, node in graph.nodes(plan).items():
        orchestration = node.get("orchestration") or {}
        if (
            node.get("kind") != "AgentNode"
            and orchestration.get("output_selection") != "exactly_one"
        ):
            errors.append(
                plan_error(
                    "unsupported_output_selection",
                    ["nodes", node_id, "orchestration", "output_selection"],
                    f"Node {node_id!r} must use output_selection 'exactly_one'.",
                )
            )
        if node.get("kind") == "HumanInputNode":
            errors.extend(check_human_input_node(node_id, node))
    return errors


def check_human_input_node(node_id: str, node: dict[str, Any]) -> list[dict[str, Any]]:
    form_path = ["nodes", node_id, "human_input", "form_schema"]
    expiry = graph.human_input_expiry_output(node)
    if expiry is not None and expiry not in graph.output_names(node):
        return [
            plan_error(
                "human_input_missing_expiry_output",
                ["nodes", node_id, "human_input", "deadline", "on_expiry", "output"],
                f"HumanInput expiry output {expiry!r} is not declared in the "
                "node's outputs.",
            )
        ]
    outputs = graph.response_outputs(node)
    if not outputs:
        return [
            plan_error(
                "no_selectable_human_input_output",
                ["nodes", node_id, "outputs"],
                f"HumanInput node {node_id!r} must declare at least one "
                "response-selectable output.",
            )
        ]
    if len(outputs) == 1:
        return []
    form_schema = (node.get("human_input") or {}).get("form_schema") or {}
    properties = form_schema.get("properties") or {}
    decision = properties.get("decision")
    if "decision" not in (form_schema.get("required") or []) or not isinstance(
        decision, dict
    ):
        return [
            plan_error(
                "incompatible_human_input_form_schema",
                form_path,
                "Multi-output HumanInput form schema must have a required "
                "top-level 'decision' property.",
            )
        ]
    choices = decision.get("enum")
    if not isinstance(choices, list) or not all(isinstance(c, str) for c in choices):
        return [
            plan_error(
                "incompatible_human_input_form_schema",
                form_path,
                "Cannot derive finite string choices for the 'decision' property.",
            )
        ]
    if expiry is not None and expiry in choices:
        return [
            plan_error(
                "human_input_expiry_output_in_decision_enum",
                form_path,
                f"Decision enum must not include the expiry-only output {expiry!r}.",
            )
        ]
    if set(choices) != set(outputs):
        return [
            plan_error(
                "incompatible_human_input_form_schema",
                form_path,
                f"Decision choices {sorted(choices)!r} do not match response "
                f"outputs {sorted(outputs)!r}.",
            )
        ]
    return []


def validate(plan: Json) -> list[dict[str, Any]]:
    """Return the errors `POST /execution-plans/validate` reports for a plan."""
    shape_errors = check_document_shape(plan)
    if shape_errors:
        return shape_errors
    return [
        *check_edges(plan),
        *check_plan_outputs(plan),
        *check_cycles(plan),
        *check_nodes(plan),
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
    deadline_at: str | None = None


@dataclass
class FakeRun:
    """One run of a plan version, and the state of each of its nodes."""

    id: str
    flow_id: str
    version: dict[str, Any]
    name: str
    parameters: dict[str, Any]
    idempotency_key: str | None
    snapshot_id: str
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
            "name": self.name,
            "parameters": self.parameters,
            "idempotency_key": self.idempotency_key,
            "state_type": "RUNNING",
            "state_name": "Running",
            "execution_plan_snapshot_id": self.snapshot_id,
        }

    def source_state(self, source: dict[str, Any]) -> str:
        """Return whether an edge source has `produced`, is `waiting`, or is `dead`."""
        if source.get("type") == "plan_input":
            name = source.get("input")
            plan_input = (self.plan.get("inputs") or {}).get(name) or {}
            if name in self.parameters or "default" in plan_input.get("schema", {}):
                return "produced"
            return "dead"
        state = self.nodes.get(str(source.get("node")))
        if state is None:
            return "dead"
        if state.status == "completed":
            return "produced" if state.output == source.get("output") else "dead"
        if state.status in ("skipped", "failed"):
            return "dead"
        return "waiting"

    def readiness(self, node_id: str) -> str:
        """Return `ready`, `waiting`, or `skip` for a pending node."""
        ports: dict[str, list[str]] = {}
        for edge in graph.edges(self.plan):
            target = graph.edge_target(edge)
            if target.get("node") == node_id:
                ports.setdefault(str(target.get("input")), []).append(
                    self.source_state(graph.edge_source(edge))
                )
        states = ["produced" if "produced" in s else s[0] for s in ports.values()]
        if any(state == "waiting" for state in states):
            return "waiting"
        if any(state == "dead" for state in states):
            return "skip"
        return "ready"

    def advance(self) -> None:
        """Move the run forward by one node, as a read of the run does."""
        order, _ = graph.topological_order(self.plan)
        node_map = graph.nodes(self.plan)
        for node_id in order:
            state = self.nodes[node_id]
            script = self.scripts.get(node_id, NodeScript())
            if state.status == "suspended" and script.expire:
                human_input = node_map[node_id].get("human_input") or {}
                on_expiry = (human_input.get("deadline") or {}).get("on_expiry")
                if on_expiry:
                    state.status = "completed"
                    state.output = on_expiry.get("output")
                    state.value = on_expiry.get("value")
                else:
                    state.status = "failed"
                    state.failure = {
                        "code": "human_input_expired",
                        "message": "The deadline passed before anyone answered.",
                    }
                return
        for node_id in order:
            state = self.nodes[node_id]
            if state.status != "pending":
                continue
            readiness = self.readiness(node_id)
            if readiness == "skip":
                state.status = "skipped"
                continue
            if readiness == "waiting":
                continue
            self.start_node(node_id, node_map[node_id])
            return

    def start_node(self, node_id: str, node: dict[str, Any]) -> None:
        state = self.nodes[node_id]
        script = self.scripts.get(node_id, NodeScript())
        state.activation_id = new_id()
        if node.get("kind") == "HumanInputNode":
            state.status = "suspended"
            state.deadline_at = (
                datetime.now(timezone.utc) + timedelta(days=3)
            ).isoformat()
            return
        if script.fail is not None:
            state.status = "failed"
            state.failure = {"code": "scripted_failure", "message": script.fail}
            return
        outputs = node.get("outputs") or {}
        output = script.output or next(iter(outputs), None)
        state.status = "completed"
        state.output = output
        state.value = (
            script.value
            if script.value is not None
            else example_value((outputs.get(output) or {}).get("schema"))
        )

    def answer(self, activation_id: str, response: dict[str, Any]) -> httpx.Response:
        for node_id, state in self.nodes.items():
            if state.activation_id != activation_id:
                continue
            if state.status != "suspended":
                return error(409, "This form is no longer waiting for an answer.")
            node = graph.nodes(self.plan)[node_id]
            form_schema = (node.get("human_input") or {}).get("form_schema") or {}
            problems = sorted(
                jsonschema.Draft202012Validator(form_schema).iter_errors(response),
                key=lambda item: list(item.absolute_path),
            )
            if problems:
                return error(
                    422, f"The response doesn't match the form: {problems[0].message}"
                )
            outputs = graph.response_outputs(node)
            output = response.get("decision") if len(outputs) > 1 else outputs[0]
            state.status = "completed"
            state.output = str(output)
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
            self.nodes[node_id].status == "pending"
            and self.readiness(node_id) == "ready"
            for node_id in self.nodes
        ):
            return "awaiting_external_progress"
        return "running"

    def plan_output(self, name: str) -> tuple[str, Json]:
        """Return a plan output's status and, when it is available, its value."""
        output = (self.plan.get("outputs") or {}).get(name) or {}
        value: dict[str, Any] = {}
        resolved = True
        for field_name, field_spec in (output.get("fields") or {}).items():
            for ref in plan_output_sources(field_spec):
                state = self.source_state(ref)
                if state == "waiting":
                    resolved = False
                elif state == "produced":
                    value[field_name] = self.nodes[str(ref.get("node"))].value
                    break
        if not resolved:
            return "waiting", None
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
                    {"output": name, "status": self.node_output_status(state, name)}
                    for name in graph.output_names(node)
                ],
            }
            if state.status == "suspended":
                body["wait"] = {
                    "kind": "human_input",
                    "activation_id": state.activation_id,
                    "form_schema": (node.get("human_input") or {}).get("form_schema"),
                    "deadline_at": state.deadline_at,
                }
            if state.failure is not None:
                body["failure"] = state.failure
            nodes.append(body)
        return {
            "snapshot": {
                "id": self.snapshot_id,
                "flow_run_id": self.id,
                "flow_id": self.flow_id,
                "execution_plan_version_id": self.version["id"],
                "schema_version": self.version["schema_version"],
                "graph_revision": 1,
            },
            "status": self.status(),
            "nodes": nodes,
            "edges": [],
            "outputs": [
                {"output": name, "status": self.plan_output(name)[0]}
                for name in self.plan.get("outputs") or {}
            ],
            "diagnostics": [],
        }

    @staticmethod
    def node_output_status(state: NodeState, name: str) -> str:
        if state.status == "completed":
            return "available" if state.output == name else "skipped"
        if state.status in ("skipped", "failed"):
            return "skipped"
        return "pending"


@dataclass
class RecordedRequest:
    method: str
    path: str
    body: Json


class FakeCloud:
    """A fake Prefect Cloud workspace API that keeps state between requests.

    `api_url` is the workspace API URL the server sends requests to. Only its
    path matters, so the same fake works behind `respx` and behind a local
    HTTP server. `scripts` sets how each node, by ID, behaves in every run.
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
        self.secret_blocks: list[dict[str, str]] = []
        self.runs: dict[str, FakeRun] = {}
        self.requests: list[RecordedRequest] = []
        self._lock = threading.Lock()
        routes: list[tuple[str, str, Handler]] = [
            ("GET", r"/execution-plans/schema", self.get_schema),
            ("POST", r"/execution-plans/validate", self.validate_plan),
            ("GET", r"/flows/name/(?P<name>[^/]+)", self.read_flow_by_name),
            ("POST", r"/flows/", self.create_flow),
            ("GET", r"/flows/(?P<flow_id>[^/]+)/execution-plan", self.read_active),
            (
                "GET",
                r"/flows/(?P<flow_id>[^/]+)/execution-plan/versions",
                self.list_versions,
            ),
            (
                "POST",
                r"/flows/(?P<flow_id>[^/]+)/execution-plan/versions",
                self.create_version,
            ),
            (
                "GET",
                r"/flows/(?P<flow_id>[^/]+)/execution-plan/versions/(?P<version_id>[^/]+)",
                self.read_version,
            ),
            (
                "POST",
                r"/flows/(?P<flow_id>[^/]+)/execution-plan/versions/(?P<version_id>[^/]+)/activate",
                self.activate_version,
            ),
            ("POST", r"/flows/(?P<flow_id>[^/]+)/execution-plan/runs", self.start_run),
            (
                "GET",
                r"/flows/(?P<flow_id>[^/]+)/execution-plan/schedules",
                self.list_schedules,
            ),
            (
                "POST",
                r"/flows/(?P<flow_id>[^/]+)/execution-plan/schedules",
                self.create_schedule,
            ),
            (
                "GET",
                r"/flows/(?P<flow_id>[^/]+)/execution-plan/schedules/(?P<schedule_id>[^/]+)",
                self.read_schedule,
            ),
            (
                "PATCH",
                r"/flows/(?P<flow_id>[^/]+)/execution-plan/schedules/(?P<schedule_id>[^/]+)",
                self.update_schedule,
            ),
            (
                "DELETE",
                r"/flows/(?P<flow_id>[^/]+)/execution-plan/schedules/(?P<schedule_id>[^/]+)",
                self.delete_schedule,
            ),
            ("GET", r"/flow_runs/(?P<run_id>[^/]+)/execution-plan", self.read_run),
            (
                "GET",
                r"/flow_runs/(?P<run_id>[^/]+)/execution-plan/outputs/(?P<name>[^/]+)",
                self.read_plan_output,
            ),
            (
                "GET",
                r"/flow_runs/(?P<run_id>[^/]+)/execution-plan/activations/(?P<activation_id>[^/]+)/outputs/(?P<name>[^/]+)",
                self.read_activation_output,
            ),
            (
                "POST",
                r"/flow_runs/(?P<run_id>[^/]+)/execution-plan/activations/(?P<activation_id>[^/]+)/human-input/responses",
                self.submit_human_input,
            ),
            ("POST", r"/block_documents/filter", self.filter_block_documents),
        ]
        self._compiled: list[tuple[str, re.Pattern[str], Handler]] = [
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
            "output_schemas": {},
        }
        self.versions.setdefault(flow_id, []).append(version)
        if activate:
            self.activate(flow_id, version)
        return version

    def activate(self, flow_id: str, version: dict[str, Any]) -> None:
        active = {key: value for key, value in version.items() if key != "created_by"}
        self.active[flow_id] = {
            **active,
            "activated": now(),
            "activated_by": FAKE_USER,
        }

    def add_schedule(
        self,
        flow_id: str,
        name: str,
        schedule: dict[str, Any],
        *,
        parameters: dict[str, Any] | None = None,
        active: bool = True,
    ) -> dict[str, Any]:
        created = now()
        body = {
            "id": new_id(),
            "flow_id": flow_id,
            "name": name,
            "active": active,
            "schedule": schedule,
            "parameters": parameters or {},
            "created": created,
            "updated": created,
            "next_scheduled_time": (
                datetime.now(timezone.utc) + timedelta(days=1)
            ).isoformat(),
            "last_created_flow_run_id": None,
            "last_error": None,
            "execution_plan_version_selection": "active_at_scheduled_time",
            "parameters_revalidated_at_scheduled_time": True,
        }
        self.schedules.setdefault(flow_id, {})[body["id"]] = body
        return body

    def add_secret_block(self, name: str) -> str:
        block_id = new_id()
        self.secret_blocks.append({"id": block_id, "name": name})
        return block_id

    # Request handling.

    def handle(self, request: httpx.Request) -> httpx.Response:
        """Answer one request to the workspace API."""
        path = unquote(request.url.path)
        if path.startswith(self.base_path):
            path = path[len(self.base_path) :] or "/"
        body = json.loads(request.content) if request.content else None
        with self._lock:
            self.requests.append(RecordedRequest(request.method, path, body))
            for method, pattern, handler in self._compiled:
                match = pattern.match(path)
                if method == request.method and match:
                    return handler(
                        body=body, params=dict(request.url.params), **match.groupdict()
                    )
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
                "validation_guidance": (
                    "This JSON Schema describes document shape only. Call "
                    "POST /execution-plans/validate before publishing a draft "
                    "because semantic validation may still reject schema-valid "
                    "documents."
                ),
            },
        )

    def validate_plan(self, *, body: Json, **_: Any) -> httpx.Response:
        errors = validate((body or {}).get("plan"))
        return json_response(200, {"valid": not errors, "errors": errors})

    def read_flow_by_name(self, *, name: str, **_: Any) -> httpx.Response:
        for flow in self.flows.values():
            if flow["name"] == name:
                return json_response(200, flow)
        return error(404, "Flow not found.")

    def create_flow(self, *, body: Json, **_: Any) -> httpx.Response:
        return json_response(201, self.add_flow(body["name"], body.get("tags")))

    def missing_flow(self, flow_id: str) -> httpx.Response | None:
        if flow_id in self.flows:
            return None
        return error(404, "Flow not found.")

    def read_active(self, *, flow_id: str, **_: Any) -> httpx.Response:
        return self.missing_flow(flow_id) or json_response(
            200, {"active_version": self.active.get(flow_id)}
        )

    def find_version(self, flow_id: str, version_id: str) -> dict[str, Any] | None:
        for version in self.versions.get(flow_id, []):
            if version["id"] == version_id:
                return version
        return None

    def list_versions(
        self, *, flow_id: str, params: dict[str, str], **_: Any
    ) -> httpx.Response:
        if missing := self.missing_flow(flow_id):
            return missing
        page = int(params.get("page", "1"))
        newest_first = list(reversed(self.versions.get(flow_id, [])))
        start = (page - 1) * VERSIONS_PAGE_SIZE
        results = [
            {
                key: value
                for key, value in version.items()
                if key not in ("plan", "output_schemas")
            }
            for version in newest_first[start : start + VERSIONS_PAGE_SIZE]
        ]
        return json_response(
            200,
            {
                "results": results,
                "count": len(newest_first),
                "page": page,
                "pages": max(1, -(-len(newest_first) // VERSIONS_PAGE_SIZE)),
            },
        )

    def create_version(self, *, flow_id: str, body: Json, **_: Any) -> httpx.Response:
        if missing := self.missing_flow(flow_id):
            return missing
        plan = (body or {}).get("plan")
        errors = validate(plan)
        if errors or not isinstance(plan, dict):
            return error(422, errors)
        return json_response(201, self.add_version(flow_id, plan, activate=False))

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
        self.activate(flow_id, version)
        return json_response(200, {"active_version": self.active[flow_id]})

    def start_run(self, *, flow_id: str, body: Json, **_: Any) -> httpx.Response:
        if missing := self.missing_flow(flow_id):
            return missing
        active = self.active.get(flow_id)
        if active is None:
            return error(404, "Active execution plan version not found.")
        body = body or {}
        key = body.get("idempotency_key")
        if key is not None:
            for run in self.runs.values():
                if run.flow_id == flow_id and run.idempotency_key == key:
                    return json_response(200, run.flow_run())
        parameters = body.get("parameters") or {}
        problems = self.parameter_problems(active["plan"], parameters)
        if problems:
            return error(422, problems)
        run = FakeRun(
            id=new_id(),
            flow_id=flow_id,
            version=self.find_version(flow_id, active["id"]) or active,
            name=body.get("name") or f"eval-run-{len(self.runs) + 1}",
            parameters=parameters,
            idempotency_key=key,
            snapshot_id=new_id(),
            scripts=self.scripts,
        )
        self.runs[run.id] = run
        return json_response(201, run.flow_run())

    @staticmethod
    def parameter_problems(
        plan: graph.Plan, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        inputs = plan.get("inputs") or {}
        problems = []
        for name, spec in inputs.items():
            if spec.get("required") and name not in parameters:
                problems.append(
                    {
                        "code": "missing_required_parameter",
                        "path": ["parameters", name],
                        "message": f"Parameter {name!r} is required.",
                    }
                )
        for name in parameters:
            if name not in inputs:
                problems.append(
                    {
                        "code": "unexpected_parameter",
                        "path": ["parameters", name],
                        "message": f"The plan has no input named {name!r}.",
                    }
                )
        return problems

    def find_run(self, run_id: str) -> FakeRun | None:
        return self.runs.get(run_id)

    def read_run(self, *, run_id: str, **_: Any) -> httpx.Response:
        run = self.find_run(run_id)
        if run is None:
            return error(404, "Flow run not found.")
        run.advance()
        return json_response(200, run.observation())

    def read_plan_output(self, *, run_id: str, name: str, **_: Any) -> httpx.Response:
        run = self.find_run(run_id)
        if run is None or name not in (run.plan.get("outputs") or {}):
            return error(404, "Output not found.")
        status, value = run.plan_output(name)
        if status == "available" and run.status() == "completed":
            return json_response(200, value)
        return self.unavailable(status if status != "available" else "waiting")

    def read_activation_output(
        self, *, run_id: str, activation_id: str, name: str, **_: Any
    ) -> httpx.Response:
        run = self.find_run(run_id)
        state = next(
            (
                state
                for state in (run.nodes.values() if run else [])
                if state.activation_id == activation_id
            ),
            None,
        )
        if state is None:
            return error(404, "Activation not found.")
        if state.status == "completed" and state.output == name:
            return json_response(200, state.value)
        status = FakeRun.node_output_status(state, name)
        return self.unavailable("skipped" if status == "skipped" else "waiting")

    @staticmethod
    def unavailable(status: str) -> httpx.Response:
        if status == "skipped":
            return json_response(
                409,
                {
                    "detail": "The output was not produced.",
                    "output_status": "skipped",
                    "reason": "not_produced",
                },
            )
        return json_response(
            409,
            {
                "detail": "The output is not available yet.",
                "output_status": "waiting",
                "reason": "not_ready",
            },
            **{"Retry-After": RETRY_AFTER_SECONDS},
        )

    def submit_human_input(
        self, *, run_id: str, activation_id: str, body: Json, **_: Any
    ) -> httpx.Response:
        run = self.find_run(run_id)
        if run is None:
            return error(404, "Flow run not found.")
        return run.answer(activation_id, (body or {}).get("response") or {})

    def list_schedules(self, *, flow_id: str, **_: Any) -> httpx.Response:
        return self.missing_flow(flow_id) or json_response(
            200, {"schedules": list(self.schedules.get(flow_id, {}).values())}
        )

    def create_schedule(self, *, flow_id: str, body: Json, **_: Any) -> httpx.Response:
        if missing := self.missing_flow(flow_id):
            return missing
        if flow_id not in self.active:
            return error(404, "Active execution plan version not found.")
        return json_response(
            201,
            self.add_schedule(
                flow_id,
                body["name"],
                body["schedule"],
                parameters=body.get("parameters"),
                active=body.get("active", True),
            ),
        )

    def read_schedule(
        self, *, flow_id: str, schedule_id: str, **_: Any
    ) -> httpx.Response:
        schedule = self.schedules.get(flow_id, {}).get(schedule_id)
        if schedule is None:
            return error(404, "Schedule not found.")
        return json_response(200, schedule)

    def update_schedule(
        self, *, flow_id: str, schedule_id: str, body: Json, **_: Any
    ) -> httpx.Response:
        schedule = self.schedules.get(flow_id, {}).get(schedule_id)
        if schedule is None:
            return error(404, "Schedule not found.")
        schedule.update(body or {})
        schedule["updated"] = now()
        return json_response(200, schedule)

    def delete_schedule(
        self, *, flow_id: str, schedule_id: str, **_: Any
    ) -> httpx.Response:
        if self.schedules.get(flow_id, {}).pop(schedule_id, None) is None:
            return error(404, "Schedule not found.")
        return httpx.Response(204)

    def filter_block_documents(self, *, body: Json, **_: Any) -> httpx.Response:
        body = body or {}
        offset = int(body.get("offset", 0))
        limit = int(body.get("limit", 200))
        blocks = sorted(self.secret_blocks, key=lambda block: block["name"])
        return json_response(
            200,
            [
                {
                    "id": block["id"],
                    "name": block["name"],
                    "block_type": {"slug": "secret", "name": "Secret"},
                    "block_type_name": "Secret",
                    "data": {},
                }
                for block in blocks[offset : offset + limit]
            ],
        )
