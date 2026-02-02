import os
import logging
import json
import pandas as pd
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

# Setup logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename='logs/ingestion.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def ingest_data():
    """
    Ingest data from nectec_demo PostgreSQL database.
    Saves data to data/landing and metadata to data/metadata.
    """
    load_dotenv()
    
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    try:
        logging.info(f"Connecting to database: {db_name}")
        engine = create_engine(connection_string)
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        logging.info(f"Connected to database. Found {len(tables)} tables: {tables}")
        print(f"✓ Connected to {db_name} database")
        print(f"✓ Found {len(tables)} tables: {', '.join(tables)}")
        
        # Create directories
        os.makedirs('data/landing', exist_ok=True)
        os.makedirs('data/metadata', exist_ok=True)
        
        for table_name in tables:
            logging.info(f"Ingesting table: {table_name}")
            print(f"\n📊 Processing table: {table_name}")
            
            # Save data to landing
            df = pd.read_sql_table(table_name, engine)
            csv_path = f'data/landing/{table_name}.csv'
            df.to_csv(csv_path, index=False)
            print(f"  ✓ Saved {len(df)} rows to {csv_path}")
            
            # Extract metadata
            columns = inspector.get_columns(table_name)
            metadata = {
                "table_name": table_name,
                "row_count": len(df),
                "column_count": len(df.columns),
                "column_info": [],
                "data_sample": df.head(5).to_dict(orient='records')
            }
            
            for col in columns:
                col_info = {
                    "column_name": col['name'],
                    "data_type": str(col['type']),
                    "nullable": col['nullable'],
                    "default": str(col['default']) if col['default'] else None,
                    "description": col.get('comment', '')
                }
                metadata["column_info"].append(col_info)
            
            # Save metadata
            def json_serial(obj):
                """JSON serializer for objects not serializable by default json code"""
                if isinstance(obj, (pd.Timestamp, pd.Series, pd.Index)):
                    return str(obj)
                return str(obj)

            metadata_path = f'data/metadata/{table_name}_metadata.json'
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=4, default=json_serial)
            print(f"  ✓ Saved metadata to {metadata_path}")
                
            logging.info(f"Successfully ingested {table_name}: {len(df)} rows, {len(df.columns)} columns")
        
        print(f"\n✅ Ingestion complete! Processed {len(tables)} tables.")
        logging.info("Data ingestion completed successfully")
            
    except Exception as e:
        logging.error(f"Error during ingestion: {str(e)}")
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    print("Starting data ingestion from nectec_demo database...")
    ingest_data()
