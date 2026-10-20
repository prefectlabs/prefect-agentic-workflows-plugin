"""Requests to the Prefect Cloud workspace in the active Prefect profile.

Every tool sends its requests through `WorkspaceApi`. It reads the API URL and
API key from the active profile on each call, so a missing or wrong profile
only fails the tool call and never stops the server from starting.

When the server runs over HTTP, a client can send its own credentials in the
`X-Prefect-Api-Url` and `X-Prefect-Api-Key` headers, and they are used instead
of the profile for that request. The headers are used as a pair, so the
profile's API key is never sent to a URL that a client chose. A client can be
anyone who reaches the server, so a header URL must use HTTPS on a host in
`PREFECT_HEADER_API_HOSTS`. Otherwise a client could make the server send
requests into the network it runs in.

Before the first request, `WorkspaceApi` runs a preflight check. The check
confirms that the profile points at a Cloud workspace and that the workspace
can use execution plans, and it turns each known failure into a message that
says how to fix it. A passing check is cached for the API URL and key it ran
with, so a new key gets checked too.
A failing check is not cached, so the next tool call runs it again after the
user fixes the problem.

The preflight check can't detect a workspace with no object storage bucket,
because Cloud only rejects requests that write to the bucket. `read_json`
turns that rejection into a message that names the missing bucket.
"""

import hashlib
import os
from dataclasses import dataclass
from typing import Any, Literal, cast

import httpx
from fastmcp.exceptions import ToolError
from fastmcp.server.dependencies import get_http_headers
from prefect.settings import Settings

HttpMethod = Literal["GET", "POST", "PATCH", "DELETE"]

DEFAULT_TIMEOUT_SECONDS = 30.0

API_URL_HEADER = "X-Prefect-Api-Url"
API_KEY_HEADER = "X-Prefect-Api-Key"
# A comma-separated list of the hosts a header URL may name. Operators set it
# to allow another Cloud host, such as staging.
HEADER_API_HOSTS_VARIABLE = "PREFECT_HEADER_API_HOSTS"
DEFAULT_HEADER_API_HOSTS = frozenset({"api.prefect.cloud"})

NO_API_URL_MESSAGE = (
    "No Prefect API URL is set in the active Prefect profile. Execution plans "
    "need a Prefect Cloud workspace. Run `prefect cloud login` and pick a "
    "workspace, then call this tool again."
)
NOT_CLOUD_MESSAGE = (
    "The Prefect API URL is {api_url}, which is not a Prefect Cloud workspace "
    "URL. A workspace URL uses HTTPS and has the form "
    "`https://<host>/api/accounts/<account_id>/workspaces/<workspace_id>`. "
    "Execution plans are only available in Prefect Cloud. Run "
    "`prefect cloud login` and pick a workspace, or switch to a Cloud profile "
    "with `prefect profile use <name>`, then call this tool again."
)
NO_API_KEY_MESSAGE = (
    "The active Prefect profile has no API key. Run `prefect cloud login` to "
    "set one, then call this tool again."
)
UNAUTHORIZED_MESSAGE = (
    "Prefect Cloud rejected the API key (HTTP 401). Run `prefect cloud login` "
    "to refresh the key in the active Prefect profile, then call this tool "
    "again."
)
FORBIDDEN_MESSAGE = (
    "Prefect Cloud refused the request to the execution-plan API (HTTP 403). "
    "The API key in the active Prefect profile may lack permission for this "
    "workspace. Check the key's role in the workspace, or run "
    "`prefect cloud login` with a different key, then call this tool again."
)
FEATURE_NOT_ENABLED_MESSAGE = (
    "Prefect Cloud returned 404 for the execution-plan API at {api_url}. "
    "Either the account and workspace in the active Prefect profile don't "
    "exist, or execution plans are not enabled for the account. Check the "
    "workspace with `prefect cloud workspace ls` and switch with "
    "`prefect cloud workspace set`. If the workspace is right, ask your "
    "Prefect contact to turn on the `execution-plans` feature for the "
    "account. Then call this tool again."
)
# Settings in the MCP server's own configuration override the profile, so the
# profile fixes above don't change them. Every setup error ends with this note.
OVERRIDE_NOTE = (
    " If the MCP server's configuration sets `PREFECT_API_URL` or "
    "`PREFECT_API_KEY`, those values are used instead of the profile: update "
    "or remove them there and restart the server."
)
HEADER_MISSING_MESSAGE = (
    "The request sets the {present} header but not the {missing} header. Send "
    "both headers, or neither to use the server's Prefect profile, then call "
    "this tool again."
)
HEADER_NOT_CLOUD_MESSAGE = (
    "The X-Prefect-Api-Url header is {api_url}, which is not a Prefect Cloud "
    "workspace URL. A workspace URL uses HTTPS and has the form "
    "`https://<host>/api/accounts/<account_id>/workspaces/<workspace_id>`. "
    "Execution plans are only available in Prefect Cloud. Fix the header in "
    "the MCP client's configuration, then call this tool again."
)
HEADER_UNTRUSTED_HOST_MESSAGE = (
    "The X-Prefect-Api-Url header is {api_url}. This server only accepts a "
    "header URL that uses HTTPS on one of these hosts: {hosts}. Fix the header "
    "in the MCP client's configuration, or ask the server's operator to add "
    "the host to `PREFECT_HEADER_API_HOSTS`, then call this tool again."
)
HEADER_UNAUTHORIZED_MESSAGE = (
    "Prefect Cloud rejected the API key in the X-Prefect-Api-Key header (HTTP "
    "401). Put a valid key in the header in the MCP client's configuration, "
    "then call this tool again."
)
HEADER_FORBIDDEN_MESSAGE = (
    "Prefect Cloud refused the request to the execution-plan API (HTTP 403). "
    "The API key in the X-Prefect-Api-Key header may lack permission for this "
    "workspace. Check the key's role in the workspace, or send a different key "
    "in the header, then call this tool again."
)
HEADER_FEATURE_NOT_ENABLED_MESSAGE = (
    "Prefect Cloud returned 404 for the execution-plan API at {api_url}. "
    "Either the account and workspace in the X-Prefect-Api-Url header don't "
    "exist, or execution plans are not enabled for the account. Check the "
    "header in the MCP client's configuration. If the workspace is right, ask "
    "your Prefect contact to turn on the `execution-plans` feature for the "
    "account. Then call this tool again."
)
NO_BUCKET_DETAIL = "object storage bucket has not been provisioned"
NO_BUCKET_MESSAGE = (
    "This workspace has no object storage bucket, so Prefect Cloud has nowhere "
    "to store execution plans (HTTP 409: {detail}). Ask your Prefect contact to "
    "configure an object storage bucket for the workspace, then call this tool "
    "again."
)
UNREACHABLE_MESSAGE = "Could not reach Prefect Cloud at {api_url}: {error}"


class RequestTimedOutError(ToolError):
    """Prefect Cloud didn't answer a request within its timeout."""


PREFLIGHT_FAILED_MESSAGE = (
    "Prefect Cloud returned HTTP {status_code} while checking that execution "
    "plans are available: {detail}"
)
NOT_JSON_MESSAGE = (
    "Prefect Cloud returned a response that is not JSON for {method} {path} "
    "(HTTP {status_code}). A proxy between you and Prefect Cloud may have "
    "answered instead."
)
REQUEST_FAILED_MESSAGE = (
    "Prefect Cloud returned HTTP {status_code} for {method} {path}: {detail}"
)


LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


@dataclass(frozen=True)
class AccessMessages:
    """Fix-it messages for preflight failures, worded for where the
    credentials came from."""

    feature_not_enabled: str
    unauthorized: str
    forbidden: str


PROFILE_MESSAGES = AccessMessages(
    feature_not_enabled=FEATURE_NOT_ENABLED_MESSAGE + OVERRIDE_NOTE,
    unauthorized=UNAUTHORIZED_MESSAGE + OVERRIDE_NOTE,
    forbidden=FORBIDDEN_MESSAGE + OVERRIDE_NOTE,
)
HEADER_MESSAGES = AccessMessages(
    feature_not_enabled=HEADER_FEATURE_NOT_ENABLED_MESSAGE,
    unauthorized=HEADER_UNAUTHORIZED_MESSAGE,
    forbidden=HEADER_FORBIDDEN_MESSAGE,
)


@dataclass(frozen=True)
class Credentials:
    """The workspace URLs and API key for one request."""

    api_url: str
    api_key: str
    ui_url: str
    messages: AccessMessages


def is_cloud_workspace_api_url(api_url: str) -> bool:
    """Return whether an API URL points at a Prefect Cloud workspace.

    The URL path must name an account and a workspace, and any host is
    accepted. The URL must use HTTPS so the API key is never sent over plain
    HTTP, except on a loopback host, where the request stays on this machine.
    """
    try:
        url = httpx.URL(api_url)
    except httpx.InvalidURL:
        return False
    if "/accounts/" not in url.path or "/workspaces/" not in url.path:
        return False
    return url.scheme == "https" or url.host in LOOPBACK_HOSTS


def header_api_hosts() -> frozenset[str]:
    """Return the hosts a URL in the `X-Prefect-Api-Url` header may name."""
    value = os.environ.get(HEADER_API_HOSTS_VARIABLE)
    if value is None:
        return DEFAULT_HEADER_API_HOSTS
    return frozenset(host.strip().lower() for host in value.split(",") if host.strip())


def is_trusted_header_api_url(api_url: str) -> bool:
    """Return whether a URL from the `X-Prefect-Api-Url` header may be used.

    Unlike a profile URL, a header URL comes from a client, so it must use
    HTTPS on an exact host from `header_api_hosts`. Loopback and other hosts
    are refused, so a client can't point the server at its own network.
    """
    url = httpx.URL(api_url)
    return url.scheme == "https" and url.host in header_api_hosts()


def header_ui_url(api_url: str) -> str:
    """Return the Prefect Cloud UI page for a workspace API URL from a header.

    This follows Prefect's own rule for a profile, which maps
    `https://api.<domain>/api/accounts/<a>/workspaces/<w>` to
    `https://app.<domain>/account/<a>/workspace/<w>`. It doesn't read the
    profile's Cloud settings, which may name a different Cloud host.
    """
    url = httpx.URL(api_url).copy_with(fragment=None)
    host = url.host
    if host.startswith("api."):
        host = "app." + host.removeprefix("api.")
    path = (
        url.path.replace("/api/", "/", 1)
        .replace("/accounts/", "/account/")
        .replace("/workspaces/", "/workspace/")
    )
    return str(url.copy_with(host=host, path=path))


def response_detail(response: httpx.Response) -> str:
    """Return the error detail from a Prefect API response as text."""
    try:
        body = response.json()
    except ValueError:
        return response.text or response.reason_phrase
    if isinstance(body, dict) and "detail" in body:
        return str(body["detail"])
    return str(body)


def read_json(response: httpx.Response, method: HttpMethod, path: str) -> Any:
    """Return the JSON body of the response to a request for `method` and `path`.

    Raises `ToolError` with the API's error detail for any status of 400 or
    higher. Returns None when the response has no body.
    """
    if response.is_error:
        detail = response_detail(response)
        # Cloud answers 409 with this detail when a request needs to write to
        # the workspace's object storage bucket and the workspace has none.
        if response.status_code == 409 and NO_BUCKET_DETAIL in detail:
            raise ToolError(NO_BUCKET_MESSAGE.format(detail=detail))
        raise ToolError(
            REQUEST_FAILED_MESSAGE.format(
                status_code=response.status_code,
                method=method,
                path=path,
                detail=detail,
            )
        )
    if not response.content:
        return None
    try:
        return response.json()
    except ValueError as exc:
        raise ToolError(
            NOT_JSON_MESSAGE.format(
                method=method, path=path, status_code=response.status_code
            )
        ) from exc


class WorkspaceApi:
    """Sends requests to the Cloud workspace in the request's headers or in
    the active Prefect profile.

    Requests go through a plain `httpx` client. The `prefect` client retries
    429 and 503 responses on its own, which would hide the `Retry-After`
    header from tools that pass it on to the agent.
    """

    def __init__(self) -> None:
        # Over HTTP, clients can send different credentials, so keep every
        # pair that passed rather than only the last one.
        self._verified: set[tuple[str, str]] = set()

    async def request(
        self,
        method: HttpMethod,
        path: str,
        *,
        json: Any = None,
        params: dict[str, Any] | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> httpx.Response:
        """Send a request to a workspace-relative path and return the response.

        The response is returned for every HTTP status, so a caller can read
        headers such as `Retry-After`. Use `call`, or pass the response to
        `read_json`, to raise on error statuses.
        Raises `ToolError` with a fix-it message when the preflight check fails.
        """
        if not path.startswith("/"):
            raise ValueError("Workspace API paths must start with '/'.")

        credentials = self._read_credentials()
        api_url, api_key = credentials.api_url, credentials.api_key
        async with httpx.AsyncClient(
            base_url=api_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout,
        ) as client:
            # Keep a digest of the key rather than the key itself.
            checked = (api_url, hashlib.sha256(api_key.encode()).hexdigest())
            if checked not in self._verified:
                await self._preflight(client, api_url, credentials.messages)
                self._verified.add(checked)
            try:
                return await client.request(method, path, json=json, params=params)
            except httpx.TimeoutException as exc:
                raise RequestTimedOutError(
                    UNREACHABLE_MESSAGE.format(api_url=api_url, error=exc)
                ) from exc
            except httpx.HTTPError as exc:
                raise ToolError(
                    UNREACHABLE_MESSAGE.format(api_url=api_url, error=exc)
                ) from exc

    async def call(
        self,
        method: HttpMethod,
        path: str,
        *,
        json: Any = None,
        params: dict[str, Any] | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> Any:
        """Send a request and return the JSON body.

        Raises `ToolError` with the API's error detail for any status of 400
        or higher. Returns None when the response has no body.
        """
        response = await self.request(
            method, path, json=json, params=params, timeout=timeout
        )
        return read_json(response, method, path)

    def current_api_url(self) -> str:
        """Return the workspace API URL from the request's headers or the
        current Prefect settings.

        Raises `ToolError` with a fix-it message when they have no Cloud
        workspace URL or no API key.
        """
        return self._read_credentials().api_url

    def current_ui_url(self) -> str:
        """Return the workspace's page in the Prefect Cloud UI.

        For a profile, Prefect works it out from the API URL, or uses
        `PREFECT_UI_URL` when that is set. For headers, it is worked out from
        the `X-Prefect-Api-Url` header. Raises `ToolError` with a fix-it
        message when there is no Cloud workspace URL or no API key.
        """
        return self._read_credentials().ui_url

    def _read_credentials(self) -> Credentials:
        # Outside an HTTP request, such as over stdio, there are no headers.
        headers = get_http_headers()
        api_url = headers.get(API_URL_HEADER.lower())
        api_key = headers.get(API_KEY_HEADER.lower())
        if not api_url and not api_key:
            return self._read_profile()
        if not api_url:
            raise ToolError(
                HEADER_MISSING_MESSAGE.format(
                    present=API_KEY_HEADER, missing=API_URL_HEADER
                )
            )
        if not api_key:
            raise ToolError(
                HEADER_MISSING_MESSAGE.format(
                    present=API_URL_HEADER, missing=API_KEY_HEADER
                )
            )
        if not is_cloud_workspace_api_url(api_url):
            raise ToolError(HEADER_NOT_CLOUD_MESSAGE.format(api_url=api_url))
        if not is_trusted_header_api_url(api_url):
            raise ToolError(
                HEADER_UNTRUSTED_HOST_MESSAGE.format(
                    api_url=api_url, hosts=", ".join(sorted(header_api_hosts()))
                )
            )
        return Credentials(
            api_url=api_url,
            api_key=api_key,
            ui_url=header_ui_url(api_url),
            messages=HEADER_MESSAGES,
        )

    def _read_profile(self) -> Credentials:
        # Load settings fresh on every call. Prefect's current-settings context
        # is fixed when the process starts, so it would miss a user switching
        # profiles or logging in to another workspace while the server runs.
        settings = Settings()
        api_url = settings.api.url
        if not api_url:
            raise ToolError(NO_API_URL_MESSAGE + OVERRIDE_NOTE)
        if not is_cloud_workspace_api_url(api_url):
            raise ToolError(NOT_CLOUD_MESSAGE.format(api_url=api_url) + OVERRIDE_NOTE)
        if settings.api.key is None or not settings.api.key.get_secret_value():
            raise ToolError(NO_API_KEY_MESSAGE + OVERRIDE_NOTE)
        return Credentials(
            api_url=api_url,
            api_key=settings.api.key.get_secret_value(),
            # SAFETY: Prefect fills `ui_url` from the API URL whenever it isn't
            # set, and the API URL is set here.
            ui_url=cast("str", settings.ui_url),
            messages=PROFILE_MESSAGES,
        )

    async def _preflight(
        self, client: httpx.AsyncClient, api_url: str, messages: AccessMessages
    ) -> None:
        # Cloud answers 404 on every execution-plan route when the account
        # does not have the `execution-plans` feature flag. Reading the
        # current schema is the cheapest request on those routes.
        try:
            response = await client.get("/execution-plans/schema")
        except httpx.HTTPError as exc:
            raise ToolError(
                UNREACHABLE_MESSAGE.format(api_url=api_url, error=exc)
            ) from exc

        if response.status_code == 404:
            raise ToolError(messages.feature_not_enabled.format(api_url=api_url))
        if response.status_code == 401:
            raise ToolError(messages.unauthorized)
        if response.status_code == 403:
            raise ToolError(messages.forbidden)
        if response.is_error:
            raise ToolError(
                PREFLIGHT_FAILED_MESSAGE.format(
                    status_code=response.status_code,
                    detail=response_detail(response),
                )
            )
