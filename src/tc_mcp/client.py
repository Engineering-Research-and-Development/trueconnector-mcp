import contextlib
import re
import time
from typing import Any

import anyio
import httpx2
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import SecretStr

from tc_mcp.config import Connector

_SAME_STATE = re.compile(r"(\w+) state can not transition to \1\b")
_EXPIRY_SKEW_S = 30


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
        self._access = connector.token.get_secret_value() if connector.token else None
        self._refresh: str | None = None
        self._expires_at = 0.0
        self._lock = anyio.Lock()

    async def request(self, method: str, path: str, *, auth: bool = True, **kwargs: Any) -> Any:
        where = f"{self.connector.name} {method} {path}"
        token = await self._token() if auth else None
        response = await self._send(method, path, token, where, **kwargs)
        if response.status_code == 401 and auth and self.connector.password is not None:
            token = await self._token(rejected=token)
            response = await self._send(method, path, token, where, **kwargs)
        body = _body(response)
        if response.is_success:
            return _unwrap(body)
        raise _error(response.status_code, body, where)

    async def _send(self, method: str, path: str, token: str | None, where: str, **kwargs: Any) -> httpx2.Response:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        try:
            return await self._http.request(method, path, headers=headers, **kwargs)
        except httpx2.TimeoutException as e:
            raise ToolError(
                f"{where}: timed out; it may still have been applied, check the state before retrying"
            ) from e
        except httpx2.TransportError as e:
            raise ToolError(f"{where}: connector unreachable at {self.connector.base_url} ({e!r})") from e

    async def aclose(self) -> None:
        if self._refresh is not None:
            with contextlib.suppress(ToolError):
                await self.request("POST", "/api/v1/auth/logout", auth=False, json={"refresh_token": self._refresh})
        await self._http.aclose()

    async def _token(self, rejected: str | None = None) -> str | None:
        if self.connector.password is None:  # static token or auth DISABLED: nothing to renew
            return self._access
        async with self._lock:  # one renewal at a time; waiters re-check and reuse its result
            if self._access is None or self._access == rejected or time.monotonic() >= self._expires_at:
                await self._renew(self.connector.password)
            return self._access

    async def _renew(self, password: SecretStr) -> None:
        if self._refresh is not None:
            try:
                tokens = await self.request(
                    "POST", "/api/v1/auth/refresh", auth=False, json={"refresh_token": self._refresh}
                )
                self._store_tokens(tokens)
                return
            except ToolError:
                pass  # refresh tokens rotate and get revoked: fall back to a full login
        credentials = {"email": self.connector.email, "password": password.get_secret_value()}
        self._store_tokens(await self.request("POST", "/api/v1/auth/login", auth=False, json=credentials))

    def _store_tokens(self, tokens: dict[str, Any]) -> None:
        self._access = tokens["access_token"]
        self._refresh = tokens["refresh_token"]
        self._expires_at = time.monotonic() + tokens["expires_in"] - _EXPIRY_SKEW_S


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
