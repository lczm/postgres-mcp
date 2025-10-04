package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var pool *pgxpool.Pool

func getExplicitBool(rawArgs map[string]interface{}, key string, argValue, defaultValue bool) bool {
	if _, exists := rawArgs[key]; exists {
		return argValue
	}
	return defaultValue
}

func getSchema(schema string) string {
	if schema == "" {
		return "public"
	}
	return schema
}

func returnJSONResult(data interface{}) (*mcp.CallToolResult, any, error) {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal results: %v", err)
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(jsonData)},
		},
	}, data, nil
}

func addOptionalString(m map[string]interface{}, key string, value *string) {
	if value != nil {
		m[key] = *value
	}
}

type QueryArgs struct {
	Query string `json:"query" jsonschema:"SQL query to execute"`
}

type TableListArgs struct {
	Schema string `json:"schema" jsonschema:"Schema name (default: public)"`
}

type TableSchemaArgs struct {
	TableName string `json:"table_name" jsonschema:"Name of the table"`
	Schema    string `json:"schema" jsonschema:"Schema name (default: public)"`
}

type TableConstraintsArgs struct {
	TableName string `json:"table_name" jsonschema:"Name of the table"`
	Schema    string `json:"schema" jsonschema:"Schema name (default: public)"`
}

type TableIndexesArgs struct {
	TableName string `json:"table_name" jsonschema:"Name of the table"`
	Schema    string `json:"schema" jsonschema:"Schema name (default: public)"`
}

type ExplainAnalyzeArgs struct {
	Query   string `json:"query" jsonschema:"SQL query to explain and analyze"`
	Analyze bool   `json:"analyze,omitempty" jsonschema:"Run ANALYZE to get actual execution statistics (default: true)"`
	Verbose bool   `json:"verbose,omitempty" jsonschema:"Include verbose output with additional details (default: false)"`
	Costs   bool   `json:"costs,omitempty" jsonschema:"Include estimated startup and total costs (default: true)"`
	Buffers bool   `json:"buffers,omitempty" jsonschema:"Include buffer usage statistics (default: false)"`
	Timing  bool   `json:"timing,omitempty" jsonschema:"Include actual timing information (default: true)"`
	Summary bool   `json:"summary,omitempty" jsonschema:"Include summary information (default: true)"`
	Format  string `json:"format,omitempty" jsonschema:"Output format: text, json, xml, or yaml (default: json)"`
}

func ExecuteQuery(ctx context.Context, req *mcp.CallToolRequest, args QueryArgs) (*mcp.CallToolResult, any, error) {
	if pool == nil {
		return nil, nil, fmt.Errorf("database not connected")
	}

	rows, err := pool.Query(ctx, args.Query)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Query error: %v", err)},
			},
			IsError: true,
		}, nil, nil
	}
	defer rows.Close()

	var results []map[string]interface{}
	fieldDescriptions := rows.FieldDescriptions()
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan row: %v", err)
		}
		row := make(map[string]interface{})
		for i, field := range fieldDescriptions {
			row[string(field.Name)] = values[i]
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("row iteration error: %v", err)
	}

	return returnJSONResult(results)
}

func ListTables(ctx context.Context, req *mcp.CallToolRequest, args TableListArgs) (*mcp.CallToolResult, any, error) {
	if pool == nil {
		return nil, nil, fmt.Errorf("database not connected")
	}

	query := `
		SELECT table_name, table_type
		FROM information_schema.tables
		WHERE table_schema = $1
		ORDER BY table_name
	`

	rows, err := pool.Query(ctx, query, getSchema(args.Schema))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list tables: %v", err)
	}
	defer rows.Close()

	var tables []map[string]interface{}
	for rows.Next() {
		var tableName, tableType string
		if err := rows.Scan(&tableName, &tableType); err != nil {
			return nil, nil, fmt.Errorf("failed to scan row: %v", err)
		}
		tables = append(tables, map[string]interface{}{
			"table_name": tableName,
			"table_type": tableType,
		})
	}

	return returnJSONResult(tables)
}

func GetTableSchema(ctx context.Context, req *mcp.CallToolRequest, args TableSchemaArgs) (*mcp.CallToolResult, any, error) {
	if pool == nil {
		return nil, nil, fmt.Errorf("database not connected")
	}

	query := `
		SELECT 
			column_name,
			data_type,
			character_maximum_length,
			is_nullable,
			column_default
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`

	rows, err := pool.Query(ctx, query, getSchema(args.Schema), args.TableName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get table schema: %v", err)
	}
	defer rows.Close()

	var columns []map[string]interface{}
	for rows.Next() {
		var columnName, dataType, isNullable string
		var maxLength, columnDefault *string

		if err := rows.Scan(&columnName, &dataType, &maxLength, &isNullable, &columnDefault); err != nil {
			return nil, nil, fmt.Errorf("failed to scan row: %v", err)
		}

		column := map[string]interface{}{
			"column_name": columnName,
			"data_type":   dataType,
			"is_nullable": isNullable,
		}
		addOptionalString(column, "max_length", maxLength)
		addOptionalString(column, "default", columnDefault)

		columns = append(columns, column)
	}

	return returnJSONResult(columns)
}

func GetTableConstraints(ctx context.Context, req *mcp.CallToolRequest, args TableConstraintsArgs) (*mcp.CallToolResult, any, error) {
	if pool == nil {
		return nil, nil, fmt.Errorf("database not connected")
	}

	query := `
		SELECT 
			tc.constraint_name,
			tc.constraint_type,
			kcu.column_name,
			ccu.table_name AS foreign_table_name,
			ccu.column_name AS foreign_column_name,
			rc.update_rule,
			rc.delete_rule,
			cc.check_clause
		FROM information_schema.table_constraints tc
		LEFT JOIN information_schema.key_column_usage kcu 
			ON tc.constraint_name = kcu.constraint_name 
			AND tc.table_schema = kcu.table_schema
		LEFT JOIN information_schema.constraint_column_usage ccu 
			ON tc.constraint_name = ccu.constraint_name 
			AND tc.table_schema = ccu.table_schema
		LEFT JOIN information_schema.referential_constraints rc 
			ON tc.constraint_name = rc.constraint_name 
			AND tc.table_schema = rc.constraint_schema
		LEFT JOIN information_schema.check_constraints cc 
			ON tc.constraint_name = cc.constraint_name 
			AND tc.table_schema = cc.constraint_schema
		WHERE tc.table_schema = $1 AND tc.table_name = $2
		ORDER BY tc.constraint_type, tc.constraint_name, kcu.ordinal_position
	`

	rows, err := pool.Query(ctx, query, getSchema(args.Schema), args.TableName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get table constraints: %v", err)
	}
	defer rows.Close()

	var constraints []map[string]interface{}
	for rows.Next() {
		var constraintName, constraintType string
		var columnName, foreignTableName, foreignColumnName, updateRule, deleteRule, checkClause *string

		if err := rows.Scan(&constraintName, &constraintType, &columnName, &foreignTableName, &foreignColumnName, &updateRule, &deleteRule, &checkClause); err != nil {
			return nil, nil, fmt.Errorf("failed to scan row: %v", err)
		}

		constraint := map[string]interface{}{
			"constraint_name": constraintName,
			"constraint_type": constraintType,
		}
		addOptionalString(constraint, "column_name", columnName)
		addOptionalString(constraint, "foreign_table_name", foreignTableName)
		addOptionalString(constraint, "foreign_column_name", foreignColumnName)
		addOptionalString(constraint, "update_rule", updateRule)
		addOptionalString(constraint, "delete_rule", deleteRule)
		addOptionalString(constraint, "check_clause", checkClause)

		constraints = append(constraints, constraint)
	}

	return returnJSONResult(constraints)
}

func GetTableIndexes(ctx context.Context, req *mcp.CallToolRequest, args TableIndexesArgs) (*mcp.CallToolResult, any, error) {
	if pool == nil {
		return nil, nil, fmt.Errorf("database not connected")
	}

	query := `
		SELECT 
			i.indexname,
			i.indexdef,
			a.amname AS index_type,
			idx.indisunique AS is_unique,
			idx.indisprimary AS is_primary,
			pg_get_indexdef(idx.indexrelid, k + 1, true) AS column_name,
			k AS column_position
		FROM pg_indexes i
		JOIN pg_class c ON c.relname = i.indexname
		JOIN pg_index idx ON idx.indexrelid = c.oid
		JOIN pg_am a ON a.oid = c.relam
		CROSS JOIN LATERAL generate_series(0, idx.indnatts - 1) AS k
		WHERE i.schemaname = $1 AND i.tablename = $2
		ORDER BY i.indexname, k
	`

	rows, err := pool.Query(ctx, query, getSchema(args.Schema), args.TableName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get table indexes: %v", err)
	}
	defer rows.Close()

	var indexes []map[string]interface{}
	for rows.Next() {
		var indexName, indexDef, indexType, columnName string
		var isUnique, isPrimary bool
		var columnPosition int

		if err := rows.Scan(&indexName, &indexDef, &indexType, &isUnique, &isPrimary, &columnName, &columnPosition); err != nil {
			return nil, nil, fmt.Errorf("failed to scan row: %v", err)
		}

		indexes = append(indexes, map[string]interface{}{
			"index_name":       indexName,
			"index_type":       indexType,
			"is_unique":        isUnique,
			"is_primary":       isPrimary,
			"column_name":      columnName,
			"column_position":  columnPosition,
			"index_definition": indexDef,
		})
	}

	return returnJSONResult(indexes)
}

func ExplainAnalyze(ctx context.Context, req *mcp.CallToolRequest, args ExplainAnalyzeArgs) (*mcp.CallToolResult, any, error) {
	if pool == nil {
		return nil, nil, fmt.Errorf("database not connected")
	}

	var rawArgs map[string]interface{}
	if req != nil {
		json.Unmarshal(req.Params.Arguments, &rawArgs)
	}
	if rawArgs == nil {
		rawArgs = make(map[string]interface{})
	}

	analyze := getExplicitBool(rawArgs, "analyze", args.Analyze, true)
	costs := getExplicitBool(rawArgs, "costs", args.Costs, true)
	timing := getExplicitBool(rawArgs, "timing", args.Timing, true)
	summary := getExplicitBool(rawArgs, "summary", args.Summary, true)
	buffers := getExplicitBool(rawArgs, "buffers", args.Buffers, false)
	verbose := getExplicitBool(rawArgs, "verbose", args.Verbose, false)

	format := args.Format
	if format == "" {
		format = "json"
	}

	// validate formats
	validFormats := map[string]bool{"text": true, "json": true, "xml": true, "yaml": true}
	if !validFormats[format] {
		format = "json"
	}

	options := []string{
		fmt.Sprintf("ANALYZE %t", analyze),
		fmt.Sprintf("COSTS %t", costs),
		fmt.Sprintf("SUMMARY %t", summary),
		fmt.Sprintf("FORMAT %s", format),
	}

	if verbose {
		options = append(options, "VERBOSE true")
	}
	if buffers {
		options = append(options, "BUFFERS true")
	}
	if analyze {
		options = append(options, fmt.Sprintf("TIMING %t", timing))
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	// always rollback, no inserts / updates / any side effects should be enabled
	defer tx.Rollback(ctx)

	explainQuery := fmt.Sprintf("EXPLAIN (%s) %s", strings.Join(options, ", "), args.Query)
	rows, err := tx.Query(ctx, explainQuery)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("EXPLAIN error: %v", err)},
			},
			IsError: true,
		}, nil, nil
	}
	defer rows.Close()

	if format == "json" {
		var results []map[string]interface{}
		fieldDescriptions := rows.FieldDescriptions()
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				return nil, nil, fmt.Errorf("failed to scan row: %v", err)
			}
			row := make(map[string]interface{})
			for i, field := range fieldDescriptions {
				row[string(field.Name)] = values[i]
			}
			results = append(results, row)
		}
		if err := rows.Err(); err != nil {
			return nil, nil, fmt.Errorf("row iteration error: %v", err)
		}
		return returnJSONResult(results)
	}

	// the rest of the formats, concatenate the rows
	var output strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, nil, fmt.Errorf("failed to scan row: %v", err)
		}
		output.WriteString(line)
		output.WriteString("\n")
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("row iteration error: %v", err)
	}

	result := output.String()
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: result},
		},
	}, result, nil
}
