# MCP PostgreSQL Server

A Model Context Protocol (MCP) server that provides tools for querying and inspecting a PostgreSQL database.

## Features

- **Execute Queries**: Run standard SQL queries (SELECT, INSERT, UPDATE, etc.)
- **List Schemas**: View available schemas in the database
- **List Tables**: See tables within a specific schema
- **Describe Tables**: Get detailed column information for any table

## Prerequisites

- Python 3.8+
- PostgreSQL database
- `pip` (Python package installer)

## Setup

1.  **Clone the repository** (if not already done)
2.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Configure Environment**:
    - Copy `.env.example` to `.env`:
      ```bash
      cp .env.example .env  # On Windows: copy .env.example .env
      ```
    - Edit `.env` and fill in your PostgreSQL credentials:
      ```ini
      DB_HOST=localhost
      DB_PORT=5432
      DB_NAME=your_database
      DB_USER=your_username
      DB_PASSWORD=your_password
      ```

## Usage

### Running Locally

To run the server locally for testing:

```bash
python server.py
```

### Testing Connection

You can verify your database connection configuration using the included test script:

```bash
python test_connection.py
```

### Integrating with MCP Client (e.g., Claude Desktop, IDEs)

Add the server configuration to your MCP client settings:

```json
{
  "mcpServers": {
    "postgres": {
      "command": "python",
      "args": ["/path/to/mcp-postgres-simple/server.py"]
    }
  }
}
```

## Tools Available

- `execute_query(query: str)`: Execute a SQL query
- `list_schemas()`: List all non-system schemas
- `list_tables(schema: str = None)`: List tables in database or specific schema
- `describe_table(table_name: str, schema: str = 'public')`: Get column details for a table

## License

[MIT](LICENSE)
