from typing import Any

import httpx2

from tc_mcp.config import Connector


class TCClient:
    def __init__(self, connector: Connector, timeout_s: float, transport: httpx2.AsyncBaseTransport | None = None):
        self.connector = connector
        self._http = httpx2.AsyncClient(base_url=connector.base_url, timeout=timeout_s, transport=transport)

    async def request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = await self._http.request(method, path, **kwargs)
        return _unwrap(_body(response))


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
