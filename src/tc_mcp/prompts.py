from mcp.server.mcpserver import MCPServer


def register(mcp: MCPServer) -> None:
    @mcp.prompt()
    def publish_dataset(title: str, description: str, external_url: str, connector: str | None = None) -> str:
        """Publish data that lives at a URL as a dataset in a connector's catalog."""
        on = f', connector="{connector}"' if connector else ""
        return f"""Publish "{title}" ({description}) as a dataset. The data is served from {external_url}.

1. Ask me which usage policy consumers must accept: a maximum number of downloads (COUNT) and/or an end date
   (DATE_TIME). Each permission becomes a separate offer a consumer can pick. Don't invent a policy.
2. Call publish_dataset(title="{title}", description="{description}", external_url="{external_url}",
   offers=<the permissions from step 1>{on}).
3. Show me the new dataset's id and its offers.

If the call fails, show me the error instead of retrying with different values."""
