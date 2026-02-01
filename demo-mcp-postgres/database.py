"""
Database connection management for MCP PostgreSQL server.
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class DatabaseManager:
    """Manages PostgreSQL database connections and queries."""
    
    def __init__(self):
        """Initialize database connection pool."""
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            1,  # minconn
            10,  # maxconn
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME', 'mcp_database'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'postgres')
        )
    
    def get_connection(self):
        """Get a connection from the pool."""
        return self.connection_pool.getconn()
    
    def release_connection(self, conn):
        """Release a connection back to the pool."""
        self.connection_pool.putconn(conn)
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as a list of dictionaries.
        
        Args:
            query: SQL query string
            params: Optional query parameters for parameterized queries
            
        Returns:
            List of dictionaries containing query results
        """
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                
                # For SELECT queries, fetch results
                if cursor.description:
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
                
                # For INSERT/UPDATE/DELETE, commit and return affected rows
                conn.commit()
                return [{"affected_rows": cursor.rowcount}]
                
        except Exception as e:
            if conn:
                conn.rollback()
            raise Exception(f"Database error: {str(e)}")
        finally:
            if conn:
                self.release_connection(conn)
    
    def list_schemas(self) -> List[Dict[str, Any]]:
        """List all non-system schemas in the database."""
        query = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schema_name;
        """
        return self.execute_query(query)
    
    def list_tables(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all tables, optionally filtered by schema.
        
        Args:
            schema: Optional schema name to filter tables
            
        Returns:
            List of dictionaries with table information
        """
        if schema:
            query = """
                SELECT 
                    table_schema,
                    table_name,
                    table_type
                FROM information_schema.tables 
                WHERE table_schema = %s
                ORDER BY table_schema, table_name;
            """
            return self.execute_query(query, (schema,))
        else:
            query = """
                SELECT 
                    table_schema,
                    table_name,
                    table_type
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name;
            """
            return self.execute_query(query)
    
    def describe_table(self, table_name: str, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Get detailed information about table columns.
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: 'public')
            
        Returns:
            List of dictionaries with column information
        """
        query = """
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """
        return self.execute_query(query, (schema, table_name))
    
    def close(self):
        """Close all connections in the pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
