import re
from typing import Any

import httpx2
from mcp.server.mcpserver.exceptions import ToolError

from tc_mcp.config import Connector

_SAME_STATE = re.compile(r"(\w+) state can not transition to \1\b")


class Rejected(ToolError):
    """The connector refused the request (HTTP 400); `reason` is its own message."""

    def __init__(self, reason: str, where: str):
        super().__init__(f"{where} rejected: {reason}")
        self.reason = reason


class AlreadyInState(Rejected):
    """A repeated lifecycle call: the object is already in `state`, nothing changed."""

    def __init__(self, state: str, reason: str, where: str):
        super().__init__(reason, where)
        self.state = state


class TCClient:
    def __init__(self, connector: Connector, timeout_s: float, transport: httpx2.AsyncBaseTransport | None = None):
        self.connector = connector
        self._http = httpx2.AsyncClient(base_url=connector.base_url, timeout=timeout_s, transport=transport)

    async def request(self, method: str, path: str, **kwargs: Any) -> Any:
        where = f"{self.connector.name} {method} {path}"
        try:
            response = await self._http.request(method, path, **kwargs)
        except httpx2.TimeoutException as e:
            raise ToolError(
                f"{where}: timed out; it may still have been applied, check the state before retrying"
            ) from e
        except httpx2.TransportError as e:
            raise ToolError(f"{where}: connector unreachable at {self.connector.base_url} ({e!r})") from e
        body = _body(response)
        if response.is_success:
            return _unwrap(body)
        raise _error(response.status_code, body, where)


def _error(status: int, body: Any, where: str) -> ToolError:
    message = body.get("message") if isinstance(body, dict) else body
    if status == 400:
        if m := _SAME_STATE.search(str(message)):
            return AlreadyInState(m.group(1), str(message), where)
        return Rejected(str(message), where)
    if status == 401:
        return ToolError(f"{where}: authentication failed, check this connector's credentials")
    if status == 403:
        return ToolError(f"{where}: forbidden, the configured account's role may not call this endpoint")
    if status >= 500:
        return ToolError(
            f"{where}: connector internal error ({message}). "
            "Usually caused by invalid input or an unreachable peer connector."
        )
    return ToolError(f"{where}: unexpected HTTP {status}: {message}")


def _body(response: httpx2.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text


def _unwrap(body: Any) -> Any:
    if isinstance(body, dict) and "response" in body:  # paged list
        data = body["response"]["data"]
        items = [{k: v for k, v in item.items() if k != "links"} for item in data["content"]]
        return {"items": items, "page": data["page"]}
    if isinstance(body, dict) and "success" in body:  # envelope
        return body["data"] if body.get("data") is not None else {"message": body["message"]}
    return body  # bare: login, dspace-version, view
