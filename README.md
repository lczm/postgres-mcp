# MCP server for postgres

There are quite a few around, but just wanted to try writing one. Sometimes instead of churning docker commands back and worth, I was thinking if it might be easier if it had access to the DB itself to just check. Of course, preferably just to check and not destroy anything :).

There are a few tools exposed to by this MCP server

- `query`: Execute SQL queries and get results as JSON
- `list_tables`: List all tables in a schema
- `get_table_schema`: Get detailed column information for a table
- `get_table_constraints`: Retrieve all constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK) for a table
- `get_table_indexes`: Get index information including index types and columns
- `explain_analyze`: Run EXPLAIN ANALYZE on queries with automatic rollback and configurable analysis options

## Installation

1. Clone this repository

```bash
git clone https://github.com/lczm/postgres-mcp
cd postgres-mcp
```

2. Install the server:

```bash
go install
```

This will build and install the `postgres-mcp` binary to your `$GOPATH/bin`

## Configuration

- I'm sure there's many different ways to set it up, with all kinds of different clients. Here's an example, I'm sure the rest is more or less similar.

Example `mcp.json`

```
{
  "servers": {
    "postgres": {
      "type": "stdio",
      "command": "${workspaceFolder}/run-mcp-docker.sh",
      "args": []
    }
  }
}
```

Update `run-mcp-docker.sh` to whatever connection string you use
