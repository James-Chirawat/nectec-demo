import os
import sys
from dotenv import load_dotenv
import psycopg2

def test_connection():
    print("Loading environment variables...")
    load_dotenv()
    
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', '5432')
    database = os.getenv('DB_NAME', 'mcp_database')
    user = os.getenv('DB_USER', 'postgres')
    # Do not print the password!
    
    print(f"Configuration:")
    print(f"  Host: {host}")
    print(f"  Port: {port}")
    print(f"  Database: {database}")
    print(f"  User: {user}")
    
    print("\nAttempting to connect to PostgreSQL...")
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=os.getenv('DB_PASSWORD', 'postgres')
        )
        print("✅ Connection successful!")
        
        # Test a simple query
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            print(f"Server version: {version}")
            
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
