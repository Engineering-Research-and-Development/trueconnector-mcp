# TrueConnector MCP server

An [MCP](https://modelcontextprotocol.io) server that lets an AI agent operate a
[TrueConnector](https://github.com/Engineering-Research-and-Development/dsp-true-connector) dataspace connector:
publish datasets, browse other connectors' catalogs, negotiate usage contracts and transfer data, through the
connector's management API. It can drive several connectors at once (e.g. a consumer and a provider) and works with
automatic and manual negotiation and transfer.

## Quick start

Needs [uv](https://docs.astral.sh/uv/) and a running TrueConnector stack (the
[dsp-true-connector](https://github.com/Engineering-Research-and-Development/dsp-true-connector) repository's Docker
setup: connector A on `localhost:8080`, connector B on `localhost:8090`).

```sh
git clone https://github.com/Engineering-Research-and-Development/trueconnector-mcp.git
cd trueconnector-mcp
cp .env.example .env          # the local stack's public demo credentials; edit for your connectors
```

Then connect a client, which starts the server itself. To run it as a standalone HTTP server instead:
`uv run tc-mcp --transport streamable-http --port 8000` (endpoint `http://localhost:8000/mcp`).

### Claude Code

```sh
claude mcp add trueconnector -- uv --directory /path/to/trueconnector-mcp run tc-mcp
```

`uv --directory` runs the server inside the repository, so it finds `.env` wherever the client starts it.

Over HTTP (a standalone server, or the Docker one below):

```sh
claude mcp add --transport http trueconnector http://localhost:8000/mcp
```

### Claude Desktop

In `claude_desktop_config.json` (on Windows, write paths as `C:/path/to/trueconnector-mcp`):

```json
{
  "mcpServers": {
    "trueconnector": {
      "command": "uv",
      "args": ["--directory", "/path/to/trueconnector-mcp", "run", "tc-mcp"]
    }
  }
}
```

Instead of `.env`, the configuration can go in an `"env"` block next to `"args"` (see [Configuration](#configuration)).

### Docker

`compose.yaml` runs the server next to the local stack, on its Docker networks, with the demo credentials written
in it; set your own before pointing it at real connectors.

```sh
docker compose up -d --build  # start the TrueConnector stack first; endpoint: http://localhost:8000/mcp
```

## Usage

Ask in plain language ("publish this CSV on connector a", "get the employee dataset from connector b"), or start one
of the [prompts](#what-the-agent-gets). In Claude Code they are slash commands:

```text
/mcp__trueconnector__consume_dataset b
```

## Configuration

Environment variables, or the same names in a `.env` file in the working directory.

| Variable | Default | Meaning |
|---|---|---|
| `TC_CONNECTORS` | required | JSON list of connectors, see below |
| `TC_READ_ONLY` | `false` | Expose only the read-only tools (and only the `diagnose_process` prompt) |
| `TC_TIMEOUT_S` | `30` | Timeout of each call to a connector, in seconds |
| `TC_MAX_CONCURRENCY` | `4` | Calls in flight per connector; further calls wait |
| `TC_MAX_COPY_MB` | `50` | Largest source `store="copy"` downloads (see [Datasets](#datasets)) |

Each connector in `TC_CONNECTORS`:

| Field | Meaning |
|---|---|
| `name` | What the agent passes as `connector` (or `peer`), e.g. `"a"` |
| `base_url` | Management API, e.g. `http://localhost:8080` |
| `protocol_url` | Address other connectors reach this one at, e.g. `http://connector-a:8080/connector-a` |
| `email`, `password` | An ADMIN user: the server logs in and renews its token itself |
| `token` | Or a fixed bearer token; with neither, no authentication is sent (connector auth disabled) |

```sh
TC_CONNECTORS='[{"name":"a","base_url":"http://localhost:8080","protocol_url":"http://connector-a:8080/connector-a","email":"admin@mail.com","password":"password"}]'
```

With more than one connector, every tool needs `connector`. Each connector call logs one line on stderr.

## What the agent gets

**54 tools**, each annotated with its effect so clients can ask before the risky ones: *read*, *repeatable*
(a repeat changes nothing more), *write*, *destructive*.

| Area | Main tools |
|---|---|
| Catalogs | `browse_remote_catalog` (another connector's offers), `get_own_catalog`, catalog CRUD |
| Datasets | `publish_dataset` (data service, distribution and dataset at once), `unpublish_dataset` (its undo), CRUD |
| Negotiation | `request_contract`, one tool per protocol step (`accept_negotiation`, `agree_negotiation`, …) |
| Transfer | `request_transfer`, one tool per protocol step (`start_transfer`, …), `get_transfer_data_url` |
| Waiting | `wait_for_state`: polls a negotiation or transfer until a state, reports silently failed data moves |
| Operations | `get_dspace_version`, `get_dashboard`, audit events |

**Resources**: `trueconnector://connectors` (names, no credentials) and, per connector, `.../catalog`,
`.../datasets/{id}`, `.../negotiations/{id}`, `.../transfers/{id}`, `.../audit/types`.

**Prompts**, step-by-step workflows that end with a summary and never decide for the user:

- `consume_dataset(provider)`: as a consumer, from choosing an offer to a download link (PULL or PUSH);
- `handle_incoming_negotiations(connector)`: as a provider, decide waiting negotiations and transfers;
- `publish_dataset(title, description, external_url)`: publish data with a policy the user chooses;
- `diagnose_process(kind, id, connector)`: read-only, why a negotiation or transfer is stuck and what would move it.

## Datasets

`publish_dataset`, `create_dataset` and `update_dataset` take the data from `external_url`, stored one of two ways:

- `store="copy"` (default): the server downloads the URL once (up to `TC_MAX_COPY_MB`) and uploads it to the
  connector as a file. Consumers get this snapshot.
- `store="link"`: only the URL is kept; the connector fetches it at transfer time. **Current TrueConnector builds
  cannot transfer such datasets** (PULL gets a 404, PUSH a 500), so use it only with a build known to fix that.

## Known limitations

- A failed data move is silent in TrueConnector (the transfer stays `STARTED`); `wait_for_state` reports it as
  `failed`, and `diagnose_process` explains it.
- Offers need at least one constraint (`COUNT` and/or `DATE_TIME`): without one, the data can never be downloaded.
- Negotiations started by the provider cannot be completed, so there is no tool for them.
- Data links point at the connector's `s3.externalPresignedEndpoint`, which must be reachable by whoever downloads.

## Security

- The server holds an ADMIN login for every configured connector. It never reaches the model: no tool or resource
  returns credentials, and logs carry no bodies, tokens or query strings.
- **The HTTP transport has no authentication**: whoever reaches the port acts as ADMIN on every connector. It
  listens on `127.0.0.1` by default (`compose.yaml` publishes it on `127.0.0.1` only). Expose it only behind an
  authenticating proxy.
- `store="copy"` makes the server fetch any http(s) URL the agent passes, from the server's network, following
  redirects. An agent misled by injected text could have it read an internal address (an intranet page, a cloud
  metadata endpoint) and publish the result. Run it where that network position is harmless, restrict its egress,
  or use `TC_READ_ONLY`.
- Catalog titles, descriptions and offers come from other parties: the prompts tell the model to treat them as data,
  never instructions, and to ask the user before every decision.
- A data link works for anyone who holds it until it expires.

## Development

```sh
uv run pytest                                           # unit tests and the API drift test, offline
uv run pytest -m integration tests/integration tests/contract  # against the local stack (.env.example)
docker compose up -d --build && uv run pytest -m integration tests/scenarios  # prompt flows over HTTP
uv run python tools/mutate.py . tools/mutants.json      # every mutant must make a test fail (slow: ~15 min)
```

The live tests create and delete their own datasets, and switch tenant automation off and back on where they need
manual mode. `tests/contract/management-api.json` is the API snapshot the drift test checks every called endpoint
against; refresh it with `curl http://localhost:8080/v3/api-docs > tests/contract/management-api.json`. CI runs
ruff, mypy, the offline tests and all mutants on every push.
