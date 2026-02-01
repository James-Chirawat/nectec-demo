"""
MCP tools for database operations.
"""
from typing import Any, Dict
from mcp.types import Tool, TextContent
import json


def get_tools() -> list[Tool]:
    """Return list of available MCP tools."""
    return [
        Tool(
            name="execute_query",
            description=(
                "Execute a SQL query on the PostgreSQL database. "
                "Supports SELECT, INSERT, UPDATE, DELETE operations. "
                "Use parameterized queries for safety."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "SQL query to execute"
                    },
                    "params": {
                        "type": "array",
                        "description": "Optional parameters for parameterized query",
                        "items": {"type": ["string", "number", "boolean", "null"]}
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="list_schemas",
            description="List all available database schemas (cs, dms, los, mls, etc.)",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="list_tables",
            description="List all tables in the database, optionally filtered by schema",
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": "Optional schema name to filter tables (e.g., 'cs', 'dms', 'los', 'mls')"
                    }
                }
            }
        ),
        Tool(
            name="describe_table",
            description="Get detailed column information for a specific table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to describe"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name (default: 'public')",
                        "default": "public"
                    }
                },
                "required": ["table_name"]
            }
        )
    ]


async def handle_execute_query(db_manager, arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle execute_query tool call."""
    query = arguments.get("query")
    params = arguments.get("params")
    
    if not query:
        return [TextContent(
            type="text",
            text=json.dumps({"error": "Query is required"}, indent=2)
        )]
    
    try:
        # Convert params list to tuple if provided
        params_tuple = tuple(params) if params else None
        results = db_manager.execute_query(query, params_tuple)
        
        return [TextContent(
            type="text",
            text=json.dumps({
                "success": True,
                "row_count": len(results),
                "data": results
            }, indent=2, default=str)
        )]
    except Exception as e:
        return [TextContent(
            type="text",
            text=json.dumps({
                "success": False,
                "error": str(e)
            }, indent=2)
        )]


async def handle_list_schemas(db_manager, arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle list_schemas tool call."""
    try:
        results = db_manager.list_schemas()
        schemas = [row['schema_name'] for row in results]
        
        return [TextContent(
            type="text",
            text=json.dumps({
                "success": True,
                "schemas": schemas
            }, indent=2)
        )]
    except Exception as e:
        return [TextContent(
            type="text",
            text=json.dumps({
                "success": False,
                "error": str(e)
            }, indent=2)
        )]


async def handle_list_tables(db_manager, arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle list_tables tool call."""
    schema = arguments.get("schema")
    
    try:
        results = db_manager.list_tables(schema)
        
        return [TextContent(
            type="text",
            text=json.dumps({
                "success": True,
                "table_count": len(results),
                "tables": results
            }, indent=2)
        )]
    except Exception as e:
        return [TextContent(
            type="text",
            text=json.dumps({
                "success": False,
                "error": str(e)
            }, indent=2)
        )]


async def handle_describe_table(db_manager, arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle describe_table tool call."""
    table_name = arguments.get("table_name")
    schema = arguments.get("schema", "public")
    
    if not table_name:
        return [TextContent(
            type="text",
            text=json.dumps({"error": "table_name is required"}, indent=2)
        )]
    
    try:
        results = db_manager.describe_table(table_name, schema)
        
        return [TextContent(
            type="text",
            text=json.dumps({
                "success": True,
                "schema": schema,
                "table": table_name,
                "columns": results
            }, indent=2)
        )]
    except Exception as e:
        return [TextContent(
            type="text",
            text=json.dumps({
                "success": False,
                "error": str(e)
            }, indent=2)
        )]
