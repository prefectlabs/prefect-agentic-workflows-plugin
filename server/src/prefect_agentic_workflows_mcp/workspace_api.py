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
    "(HTTP {status_code}). Run `prefect cloud login` to refresh it, then call "
    "this tool again."
)
FEATURE_NOT_ENABLED_MESSAGE = (
    "Execution plans are not enabled for this workspace's account. Prefect "
    "Cloud returned 404 for the execution-plan API. Ask your Prefect contact "
    "to turn on the `execution-plans` feature for the account, then call this "
    "tool again."
)
UNREACHABLE_MESSAGE = "Could not reach Prefect Cloud at {api_url}: {error}"
PREFLIGHT_FAILED_MESSAGE = (
    "Prefect Cloud returned HTTP {status_code} while checking that execution "
    "plans are available: {detail}"
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
        headers such as `Retry-After`. Use `call` to raise on error statuses.
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
        if response.is_error:
            raise ToolError(
                REQUEST_FAILED_MESSAGE.format(
                    status_code=response.status_code,
                    method=method,
                    path=path,
                    detail=response_detail(response),
                )
            )
        if not response.content:
            return None
        return response.json()

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
        if response.status_code in (401, 403):
            raise ToolError(
                UNAUTHORIZED_MESSAGE.format(status_code=response.status_code)
            )
        if response.is_error:
            raise ToolError(
                PREFLIGHT_FAILED_MESSAGE.format(
                    status_code=response.status_code,
                    detail=response_detail(response),
                )
            )
