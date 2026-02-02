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

def analyze_data():
    """
    Analyze data from data/landing directory.
    Performs exploratory data analysis and saves results.
    """
    landing_dir = 'data/landing'
    analysis_results = {}
    
    if not os.path.exists(landing_dir):
        print(f"❌ Error: {landing_dir} directory not found. Please run ingest.py first.")
        logging.error(f"{landing_dir} directory not found")
        return
    
    files = [f for f in os.listdir(landing_dir) if f.endswith('.csv')]
    
    if not files:
        print(f"❌ Error: No CSV files found in {landing_dir}. Please run ingest.py first.")
        logging.error(f"No CSV files found in {landing_dir}")
        return
    
    print(f"Starting analysis of {len(files)} tables...")
    logging.info(f"Starting analysis of {len(files)} tables")
    
    for file in files:
        table_name = file.replace('.csv', '')
        logging.info(f"Analyzing table: {table_name}")
        print(f"\n📊 Analyzing: {table_name}")
        
        try:
            df = pd.read_csv(os.path.join(landing_dir, file))
            
            # Basic statistics
            analysis_results[table_name] = {
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": df.columns.tolist(),
                "missing_values": df.isnull().sum().to_dict(),
                "column_types": df.dtypes.astype(str).to_dict()
            }
            
            # Numeric summary if numeric columns exist
            numeric_cols = df.select_dtypes(include=['number'])
            if not numeric_cols.empty:
                analysis_results[table_name]["numeric_summary"] = df.describe().to_dict()
                print(f"  ✓ {len(df)} rows, {len(df.columns)} columns ({len(numeric_cols.columns)} numeric)")
            else:
                analysis_results[table_name]["numeric_summary"] = "No numeric columns"
                print(f"  ✓ {len(df)} rows, {len(df.columns)} columns (no numeric columns)")
            
            # Missing values summary
            missing_count = df.isnull().sum().sum()
            if missing_count > 0:
                print(f"  ⚠️  {missing_count} missing values found")
            else:
                print(f"  ✓ No missing values")
                
            logging.info(f"Successfully analyzed {table_name}")
            
        except Exception as e:
            logging.error(f"Error analyzing {table_name}: {str(e)}")
            print(f"  ❌ Error: {e}")
    
    # Save results
    os.makedirs('data', exist_ok=True)
    output_file = 'data/analysis_results.json'
    
    with open(output_file, 'w') as f:
        json.dump(analysis_results, f, indent=4)
    
    print(f"\n✅ Analysis complete! Results saved to {output_file}")
    logging.info(f"Analysis results saved to {output_file}")

if __name__ == "__main__":
    analyze_data()
