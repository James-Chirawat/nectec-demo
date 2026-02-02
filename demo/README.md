# NECTEC Demo - Data Pipeline

A simple Python data pipeline for extracting, analyzing, and visualizing PostgreSQL database data.

## 📋 Overview

This pipeline consists of three main components:
1. **Data Ingestion** (`ingest.py`) - Extracts data from PostgreSQL
2. **Data Analysis** (`analyze.py`) - Performs exploratory data analysis
3. **Dashboard** (`app.py`) - Interactive Streamlit visualization

## 📁 Project Structure

```
demo/
├── .env                      # Database credentials
├── ingest.py                 # Data ingestion script
├── analyze.py                # Data analysis script
├── app.py                    # Streamlit dashboard
├── requirements.txt          # Python dependencies
├── data/
│   ├── landing/             # Extracted CSV files
│   ├── metadata/            # Metadata JSON files
│   └── analysis_results.json # Analysis output
└── logs/
    ├── ingestion.log        # Ingestion logs
    └── analysis.log         # Analysis logs
```

## 🚀 Setup

### Prerequisites
- Python 3.7+
- PostgreSQL server running
- Database named `nectec_demo` (or update `.env` file)

### Installation

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure database connection:**
   
   Edit `.env` file with your database credentials:
   ```
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=nectec_demo
   DB_USER=postgres
   DB_PASSWORD=postgres
   ```

3. **Create the database (if it doesn't exist):**
   ```bash
   psql -U postgres -c "CREATE DATABASE nectec_demo;"
   ```

## 📊 Usage

### Step 1: Ingest Data
Extract data from the database and save to local files:
```bash
python ingest.py
```

This will:
- Connect to the `nectec_demo` database
- Extract all tables as CSV files to `data/landing/`
- Save metadata (column info, types, samples) to `data/metadata/`
- Log operations to `logs/ingestion.log`

### Step 2: Analyze Data
Perform exploratory data analysis:
```bash
python analyze.py
```

This will:
- Read data from `data/landing/`
- Generate statistics (row counts, missing values, data types)
- Save results to `data/analysis_results.json`
- Log operations to `logs/analysis.log`

### Step 3: View Dashboard
Launch the interactive dashboard:
```bash
streamlit run app.py
```

The dashboard provides:
- **Overview Tab**: Statistics, missing values charts, numeric summaries
- **Metadata Tab**: Column definitions and sample data
- **Data Preview Tab**: Browse table data with download option

## 🔧 Troubleshooting

### Database Connection Error
If you get `database "nectec_demo" does not exist`:
1. Create the database: `psql -U postgres -c "CREATE DATABASE nectec_demo;"`
2. Or update `DB_NAME` in `.env` to an existing database

### Check Available Databases
Run the helper script:
```bash
python check_databases.py
```

## 📝 Notes

- The pipeline uses simple Python structure (no uv or complex build tools)
- All dependencies are listed in `requirements.txt`
- Logs are saved to the `logs/` folder for debugging
- Data is saved locally in the `data/` folder
