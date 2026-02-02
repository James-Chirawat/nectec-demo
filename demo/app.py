import streamlit as st
import pandas as pd
import json
import os
import plotly.express as px

st.set_page_config(page_title="NECTEC Demo Dashboard", layout="wide", page_icon="📊")

st.title("📊 NECTEC Demo - Data Dashboard")

# Load analysis results
analysis_file = 'data/analysis_results.json'
if not os.path.exists(analysis_file):
    st.error("❌ Analysis results not found. Please run `ingest.py` and `analyze.py` first.")
    st.code("""
    # Run these commands:
    python ingest.py
    python analyze.py
    """)
    st.stop()

with open(analysis_file, 'r') as f:
    analysis_data = json.load(f)

# Sidebar navigation
st.sidebar.header("Navigation")
tables = list(analysis_data.keys())

if not tables:
    st.error("No tables found in analysis results.")
    st.stop()

selected_table = st.sidebar.selectbox("📋 Select Table", tables)

# Main content
if selected_table:
    st.header(f"Table: `{selected_table}`")
    
    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["📈 Overview", "📝 Metadata", "🔍 Data Preview"])
    
    # Tab 1: Overview
    with tab1:
        st.subheader("Statistical Summary")
        
        # Metrics
        table_stats = analysis_data[selected_table]
        col1, col2, col3 = st.columns(3)
        
        col1.metric("📊 Row Count", f"{table_stats['row_count']:,}")
        col2.metric("📋 Column Count", table_stats['column_count'])
        
        # Count missing values
        total_missing = sum(table_stats['missing_values'].values())
        col3.metric("⚠️ Missing Values", f"{total_missing:,}")
        
        # Missing values chart
        if total_missing > 0:
            st.write("### Missing Values by Column")
            missing_df = pd.DataFrame(
                [(k, v) for k, v in table_stats['missing_values'].items() if v > 0],
                columns=['Column', 'Missing Count']
            )
            fig = px.bar(
                missing_df, 
                x='Column', 
                y='Missing Count',
                title="Missing Values Distribution",
                color='Missing Count',
                color_continuous_scale='Reds'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.success("✅ No missing values found in this table!")
        
        # Numeric summary
        if table_stats['numeric_summary'] != "No numeric columns":
            st.write("### Numeric Columns Statistics")
            numeric_df = pd.DataFrame(table_stats['numeric_summary'])
            st.dataframe(numeric_df, use_container_width=True)
        else:
            st.info("ℹ️ This table has no numeric columns")
        
        # Column types
        st.write("### Column Data Types")
        types_df = pd.DataFrame(
            table_stats['column_types'].items(),
            columns=['Column Name', 'Data Type']
        )
        st.dataframe(types_df, use_container_width=True)
    
    # Tab 2: Metadata
    with tab2:
        st.subheader("Metadata Information")
        
        metadata_file = f'data/metadata/{selected_table}_metadata.json'
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                meta = json.load(f)
            
            st.write("#### Table Information")
            info_col1, info_col2 = st.columns(2)
            info_col1.write(f"**Table Name:** `{meta['table_name']}`")
            info_col2.write(f"**Total Rows:** {meta['row_count']:,}")
            
            st.write("#### Column Definitions")
            col_info_df = pd.DataFrame(meta['column_info'])
            st.dataframe(col_info_df, use_container_width=True)
            
            st.write("#### Sample Data")
            sample_df = pd.DataFrame(meta['data_sample'])
            st.dataframe(sample_df, use_container_width=True)
        else:
            st.warning(f"⚠️ Metadata file not found: {metadata_file}")
    
    # Tab 3: Data Preview
    with tab3:
        st.subheader("Data Preview")
        
        landing_file = f'data/landing/{selected_table}.csv'
        if os.path.exists(landing_file):
            df = pd.read_csv(landing_file)
            
            # Show number of rows
            st.write(f"**Total rows:** {len(df):,}")
            
            # Row limit selector
            row_limit = st.slider("Number of rows to display", 10, min(500, len(df)), 100)
            
            st.dataframe(df.head(row_limit), use_container_width=True)
            
            # Download button
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Full CSV",
                data=csv,
                file_name=f"{selected_table}.csv",
                mime="text/csv"
            )
        else:
            st.error(f"❌ Data file not found: {landing_file}")

# Sidebar info
st.sidebar.markdown("---")
st.sidebar.info("""
**NECTEC Demo Dashboard**

This dashboard displays data ingested from the `nectec_demo` PostgreSQL database.

**Pipeline:**
1. `ingest.py` - Extract data
2. `analyze.py` - Analyze data  
3. `app.py` - Visualize data
""")
