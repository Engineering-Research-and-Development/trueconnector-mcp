import argparse
import logging
import sys

from tc_mcp.config import Settings
from tc_mcp.server import build_server


def main() -> None:
    parser = argparse.ArgumentParser(prog="tc-mcp", description="MCP server for TrueConnector")
    parser.add_argument("--transport", choices=["stdio", "streamable-http"], default="stdio")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, stream=sys.stderr)  # stdout is the stdio protocol channel
    server = build_server(Settings())
    if args.transport == "stdio":
        server.run("stdio")
    else:
        server.run("streamable-http", host=args.host, port=args.port)


if __name__ == "__main__":
    main()
