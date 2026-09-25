"""Requests to the Prefect Cloud workspace in the active Prefect profile.

Every tool sends its requests through `WorkspaceApi`. It reads the API URL and
API key from the active profile on each call, so a missing or wrong profile
only fails the tool call and never stops the server from starting.

Before the first request, `WorkspaceApi` runs a preflight check. The check
confirms that the profile points at a Cloud workspace and that the workspace
can use execution plans, and it turns each known failure into a message that
says how to fix it. A passing check is cached for the API URL it ran against.
A failing check is not cached, so the next tool call runs it again after the
user fixes the problem.

The preflight check can't detect a workspace with no object storage bucket,
because Cloud only rejects requests that write to the bucket. `read_json`
turns that rejection into a message that names the missing bucket.
"""

from typing import Any, Literal

import httpx
from fastmcp.exceptions import ToolError
from prefect.settings import get_current_settings

HttpMethod = Literal["GET", "POST", "PATCH", "DELETE"]

DEFAULT_TIMEOUT_SECONDS = 30.0

NO_API_URL_MESSAGE = (
    "No Prefect API URL is set in the active Prefect profile. Execution plans "
    "need a Prefect Cloud workspace. Run `prefect cloud login` and pick a "
    "workspace, then call this tool again."
)
NOT_CLOUD_MESSAGE = (
    "The active Prefect profile points at {api_url}, which is not a Prefect "
    "Cloud workspace URL. Execution plans are only available in Prefect Cloud. "
    "Run `prefect cloud login` and pick a workspace, or switch to a Cloud "
    "profile with `prefect profile use <name>`, then call this tool again."
)
NO_API_KEY_MESSAGE = (
    "The active Prefect profile has no API key. Run `prefect cloud login` to "
    "set one, then call this tool again."
)
UNAUTHORIZED_MESSAGE = (
    "Prefect Cloud rejected the API key in the active Prefect profile "
    "(HTTP 401). Run `prefect cloud login` to refresh it, then call this tool "
    "again."
)
FORBIDDEN_MESSAGE = (
    "Prefect Cloud refused the request to the execution-plan API (HTTP 403). "
    "The API key in the active Prefect profile may lack permission for this "
    "workspace. Check the key's role in the workspace, or run "
    "`prefect cloud login` with a different key, then call this tool again."
)
FEATURE_NOT_ENABLED_MESSAGE = (
    "Execution plans are not enabled for this workspace's account. Prefect "
    "Cloud returned 404 for the execution-plan API. Ask your Prefect contact "
    "to turn on the `execution-plans` feature for the account, then call this "
    "tool again."
)
NO_BUCKET_DETAIL = "object storage bucket has not been provisioned"
NO_BUCKET_MESSAGE = (
    "This workspace has no object storage bucket, so Prefect Cloud has nowhere "
    "to store execution plans (HTTP 409: {detail}). Ask your Prefect contact to "
    "configure an object storage bucket for the workspace, then call this tool "
    "again."
)
UNREACHABLE_MESSAGE = "Could not reach Prefect Cloud at {api_url}: {error}"
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


def is_cloud_workspace_api_url(api_url: str) -> bool:
    """Return whether an API URL points at a Prefect Cloud workspace."""
    return "/accounts/" in api_url and "/workspaces/" in api_url


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
    """Sends requests to the Cloud workspace in the active Prefect profile.

    Requests go through a plain `httpx` client. The `prefect` client retries
    429 and 503 responses on its own, which would hide the `Retry-After`
    header from tools that pass it on to the agent.
    """

    def __init__(self) -> None:
        self._verified_api_url: str | None = None

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

        api_url, api_key = self._read_profile()
        async with httpx.AsyncClient(
            base_url=api_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout,
        ) as client:
            if self._verified_api_url != api_url:
                await self._preflight(client, api_url)
                self._verified_api_url = api_url
            try:
                return await client.request(method, path, json=json, params=params)
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

    def _read_profile(self) -> tuple[str, str]:
        settings = get_current_settings()
        api_url = settings.api.url
        if not api_url:
            raise ToolError(NO_API_URL_MESSAGE)
        if not is_cloud_workspace_api_url(api_url):
            raise ToolError(NOT_CLOUD_MESSAGE.format(api_url=api_url))
        if settings.api.key is None or not settings.api.key.get_secret_value():
            raise ToolError(NO_API_KEY_MESSAGE)
        return api_url, settings.api.key.get_secret_value()

    async def _preflight(self, client: httpx.AsyncClient, api_url: str) -> None:
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
            raise ToolError(FEATURE_NOT_ENABLED_MESSAGE)
        if response.status_code == 401:
            raise ToolError(UNAUTHORIZED_MESSAGE)
        if response.status_code == 403:
            raise ToolError(FORBIDDEN_MESSAGE)
        if response.is_error:
            raise ToolError(
                PREFLIGHT_FAILED_MESSAGE.format(
                    status_code=response.status_code,
                    detail=response_detail(response),
                )
            )
