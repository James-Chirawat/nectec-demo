import os
import pandas as pd
import json
import logging

# Setup logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename='logs/analysis.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, LabelEncoder
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

def analyze_data():
    load_dotenv()
    landing_dir = 'data/landing'
    analysis_results = {}
    
    # 1. Basic EDA (Existing logic)
    if os.path.exists(landing_dir):
        files = [f for f in os.listdir(landing_dir) if f.endswith('.csv')]
        for file in files:
            table_name = file.replace('.csv', '')
            logging.info(f"Analyzing table: {table_name}")
            try:
                df = pd.read_csv(os.path.join(landing_dir, file))
                analysis_results[table_name] = {
                    "row_count": len(df),
                    "column_count": len(df.columns),
                    "missing_values": df.isnull().sum().to_dict(),
                    "numeric_summary": df.describe().to_dict() if not df.select_dtypes(include=['number']).empty else "No numeric columns",
                    "column_types": df.dtypes.astype(str).to_dict()
                }
            except Exception as e:
                logging.error(f"Error analyzing {table_name}: {str(e)}")

    # 2. Advanced Analysis (Target Queries)
    target_results = {}
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    try:
        engine = create_engine(connection_string)
        queries = {
            "top_albums": """
                SELECT a.title, SUM(t.unit_price) as album_value
                FROM album a
                JOIN track t ON a.album_id = t.album_id
                GROUP BY a.album_id, a.title
                ORDER BY album_value DESC
                LIMIT 5;
            """,
            "revenue_by_country": """
                SELECT billing_country, SUM(total) as total_revenue, COUNT(invoice_id) as sales_count
                FROM invoice
                GROUP BY billing_country
                ORDER BY total_revenue DESC;
            """,
            "employee_hierarchy": """
                SELECT 
                    e.first_name || ' ' || e.last_name AS employee,
                    m.first_name || ' ' || m.last_name AS reports_to_manager
                FROM employee e
                LEFT JOIN employee m ON e.reports_to = m.employee_id;
            """
        }

        for key, query in queries.items():
            logging.info(f"Executing query: {key}")
            with engine.connect() as conn:
                df_target = pd.read_sql(text(query), conn)
                target_results[key] = df_target.to_dict(orient='records')

        # 3. Machine Learning (K-Means Clustering)
        logging.info("Performing customer clustering...")
        customer_query = """
            SELECT 
                customer_id,
                COUNT(invoice_id) as total_invoices,
                SUM(total) as total_spent
            FROM invoice
            GROUP BY customer_id;
        """
        with engine.connect() as conn:
            df_customers = pd.read_sql(text(customer_query), conn)
        
        if not df_customers.empty:
            # Preprocessing
            X_clust = df_customers[['total_invoices', 'total_spent']]
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X_clust)
            
            # K-Means
            kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
            df_customers['cluster'] = kmeans.fit_predict(X_scaled)
            
            target_results['customer_clusters'] = df_customers.to_dict(orient='records')
            logging.info("Customer clustering complete.")

        # 4. LightGBM (Spending Prediction)
        logging.info("Training LightGBM model...")
        predict_query = """
            SELECT 
                c.customer_id,
                c.country,
                c.support_rep_id,
                COUNT(i.invoice_id) as total_invoices,
                SUM(i.total) as total_spent
            FROM customer c
            JOIN invoice i ON c.customer_id = i.customer_id
            GROUP BY c.customer_id, c.country, c.support_rep_id;
        """
        with engine.connect() as conn:
            df_pred = pd.read_sql(text(predict_query), conn)
            
        if not df_pred.empty:
            # Encodings
            le_country = LabelEncoder()
            df_pred['country_code'] = le_country.fit_transform(df_pred['country'])
            
            # Features and Target
            X = df_pred[['country_code', 'support_rep_id', 'total_invoices']]
            y = df_pred['total_spent']
            
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            # Model
            model = lgb.LGBMRegressor(n_estimators=100, random_state=42, verbose=-1)
            model.fit(X_train, y_train)
            
            # Prediction
            y_pred = model.predict(X_test)
            
            # Results
            target_results['prediction_results'] = {
                "r2_score": r2_score(y_test, y_pred),
                "mse": mean_squared_error(y_test, y_pred),
                "feature_importance": dict(zip(X.columns.tolist(), model.feature_importances_.tolist())),
                "actual_vs_pred": pd.DataFrame({
                    "actual": y_test.tolist(),
                    "predicted": y_pred.tolist()
                }).to_dict(orient='records')
            }
            logging.info("LightGBM training complete.")

    except Exception as e:
        logging.error(f"Error during advanced analysis/ML: {str(e)}")

    # Save results
    os.makedirs('data', exist_ok=True)
    with open('data/analysis_results.json', 'w') as f:
        json.dump({
            "basic_eda": analysis_results,
            "target_analysis": target_results
        }, f, indent=4)
        
    logging.info("All analysis results saved to data/analysis_results.json")




if __name__ == "__main__":
    analyze_data()
