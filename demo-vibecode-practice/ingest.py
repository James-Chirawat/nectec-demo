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
    load_dotenv()
    
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    try:
        engine = create_engine(connection_string)
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        logging.info(f"Connected to database. Found tables: {tables}")
        
        os.makedirs('data/landing', exist_ok=True)
        os.makedirs('data/metadata', exist_ok=True)
        
        for table_name in tables:
            logging.info(f"Ingesting table: {table_name}")
            
            # Save data to landing
            df = pd.read_sql_table(table_name, engine)
            df.to_csv(f'data/landing/{table_name}.csv', index=False)
            
            # Extract metadata
            columns = inspector.get_columns(table_name)
            metadata = {
                "table_name": table_name,
                "column_info": [],
                "data_sample": df.head(5).to_dict(orient='records')
            }
            
            for col in columns:
                metadata["column_info"].append({
                    "column_name": col['name'],
                    "data_type": str(col['type']),
                    "nullable": col['nullable'],
                    "default": str(col['default']) if col['default'] else None,
                    "description": col.get('comment', '')
                })
            
            # Save metadata
            def json_serial(obj):
                """JSON serializer for objects not serializable by default json code"""
                if isinstance(obj, (pd.Timestamp, pd.Series, pd.Index)):
                    return str(obj)
                return str(obj)

            with open(f'data/metadata/{table_name}_metadata.json', 'w') as f:
                json.dump(metadata, f, indent=4, default=json_serial)
                
            logging.info(f"Successfully ingested {table_name}")
            
    except Exception as e:
        logging.error(f"Error during ingestion: {str(e)}")
        print(f"Error: {e}")

if __name__ == "__main__":
    ingest_data()
