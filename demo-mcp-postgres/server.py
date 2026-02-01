#!/usr/bin/env python3
"""
MCP Server for PostgreSQL Database.

This server provides tools for querying and inspecting a PostgreSQL database
through the Model Context Protocol (MCP).
"""
import asyncio
import logging
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent

from database import DatabaseManager
from tools import (
    get_tools,
    handle_execute_query,
    handle_list_schemas,
    handle_list_tables,
    handle_describe_table
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-database")

# Initialize database manager
db_manager = DatabaseManager()


async def main():
    """Main entry point for the MCP server."""
    logger.info("Starting MCP Database Server")
    
    # Create MCP server instance
    server = Server("mcp-database")
    
    @server.list_tools()
    async def list_tools():
        """List available tools."""
        return get_tools()
    
    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        """Handle tool calls."""
        logger.info(f"Tool called: {name} with arguments: {arguments}")
        
        try:
            if name == "execute_query":
                return await handle_execute_query(db_manager, arguments)
            elif name == "list_schemas":
                return await handle_list_schemas(db_manager, arguments)
            elif name == "list_tables":
                return await handle_list_tables(db_manager, arguments)
            elif name == "describe_table":
                return await handle_describe_table(db_manager, arguments)
            else:
                return [TextContent(
                    type="text",
                    text=f"Unknown tool: {name}"
                )]
        except Exception as e:
            logger.error(f"Error handling tool call: {e}")
            return [TextContent(
                type="text",
                text=f"Error: {str(e)}"
            )]
    
    # Run the server
    async with stdio_server() as (read_stream, write_stream):
        logger.info("Server running with stdio transport")
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server shutdown requested")
    finally:
        db_manager.close()
        logger.info("Database connections closed")
