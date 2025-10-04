package main

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = os.Getenv("POSTGRES_URL")
	}
	if connStr == "" {
		log.Fatal("DATABASE_URL or POSTGRES_URL environment variable must be set")
	}

	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		log.Fatalf("Failed to parse database URL: %v", err)
	}

	ctx := context.Background()
	pool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		log.Fatalf("Failed to create connection pool: %v", err)
	}
	defer pool.Close()

	// try to connect, otherwise fail
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
		return
	}

	server := mcp.NewServer(&mcp.Implementation{
		Name:    "postgres-mcp",
		Version: "v1.0.0",
	}, nil)

	// tools that are available
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_table_schema",
		Description: "Get the schema information (columns, data types, etc.) for a specific table",
	}, GetTableSchema)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "query",
		Description: "Execute a SQL query against the PostgreSQL database and return results as JSON",
	}, ExecuteQuery)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_tables",
		Description: "List all tables in the specified schema (default: public)",
	}, ListTables)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_table_constraints",
		Description: "Get all constraints (primary key, foreign key, unique, check) for a specific table",
	}, GetTableConstraints)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_table_indexes",
		Description: "Get all indexes for a specific table including index type and columns",
	}, GetTableIndexes)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "explain_analyze",
		Description: "Run EXPLAIN ANALYZE on a query to get the query execution plan and performance metrics. Supports options for analyze, verbose, costs, buffers, timing, summary, and output format (text, json, xml, yaml)",
	}, ExplainAnalyze)

	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		log.Fatal(err)
	}
}
