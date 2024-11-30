import streamlit as st
import pandas as pd
from pathlib import Path
import logging
from data_processor import process_and_insert_data, init_db
import tempfile
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

st.set_page_config(
    page_title="Data Import Tool",
    page_icon="📊",
    layout="wide"
)

def save_uploaded_file(uploaded_file):
    """Save uploaded file to temp directory and return path"""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uploaded_file.name).suffix) as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            return tmp_file.name
    except Exception as e:
        st.error(f"Error saving uploaded file: {e}")
        return None

def main():
    st.title("Data Import Tool")
    
    # Initialize database
    try:
        init_db()
        st.success("Database initialized successfully")
    except Exception as e:
        st.error(f"Database initialization failed: {e}")
        return

    # File upload section
    st.header("Upload Data File")
    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=['csv', 'xlsx', 'xls'])
    
    if uploaded_file:
        try:
            # Display file info
            st.write("File Details:")
            st.json({
                "Filename": uploaded_file.name,
                "Size": f"{uploaded_file.size/1024:.2f} KB",
                "Type": uploaded_file.type
            })

            # Preview data
            if uploaded_file.type == "text/csv":
                df_preview = pd.read_csv(uploaded_file)
            else:
                df_preview = pd.read_excel(uploaded_file, skiprows=1)
            
            st.write("### Data Preview")
            
            # Display basic statistics
            st.write("#### Dataset Overview")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Rows", len(df_preview))
            with col2:
                st.metric("Total Columns", len(df_preview.columns))
            with col3:
                st.metric("Memory Usage", f"{df_preview.memory_usage(deep=True).sum() / 1024:.2f} KB")
            
            # Show timestamp columns if any
            date_cols = df_preview.select_dtypes(include=['datetime64']).columns
            if len(date_cols) > 0:
                st.write("#### Timestamp Analysis")
                st.write("Detected timestamp columns:", ", ".join(date_cols))
                for col in date_cols:
                    st.write(f"**{col} - Date Range:**")
                    min_date = df_preview[col].min()
                    max_date = df_preview[col].max()
                    st.write(f"From: {min_date}")
                    st.write(f"To: {max_date}")
            
            # Display interactive data table
            st.write("#### Data Table (First 100 rows)")
            st.dataframe(df_preview.head(100), use_container_width=True)
            
            # Show column info
            st.write("#### Column Information")
            col_info = pd.DataFrame({
                'Column': df_preview.columns,
                'Type': df_preview.dtypes,
                'Non-Null Count': df_preview.count(),
                'Null Count': df_preview.isnull().sum()
            })
            st.dataframe(col_info, use_container_width=True)

            # Process button
            if st.button("Process and Import Data"):
                with st.spinner("Processing data..."):
                    # Save uploaded file to temp location
                    temp_path = save_uploaded_file(uploaded_file)
                    if temp_path:
                        try:
                            # Process the file
                            result = process_and_insert_data(Path(temp_path))
                            st.success("Data imported successfully!")
                            
                            # Display import statistics
                            if result:
                                st.write("Import Statistics:")
                                st.json(result)
                        except Exception as e:
                            st.error(f"Error processing data: {e}")
                        finally:
                            # Cleanup temp file
                            os.unlink(temp_path)
                    else:
                        st.error("Failed to save uploaded file")

        except Exception as e:
            st.error(f"Error reading file: {e}")

    # Help section
    with st.expander("Help & Instructions"):
        st.markdown("""
        ### How to use this tool:
        1. Upload a CSV or Excel file containing your data
        2. Preview the data to ensure it's correct
        3. Click 'Process and Import Data' to start the import process
        
        ### Supported file types:
        - CSV (.csv)
        - Excel (.xlsx, .xls)
        
        ### Data requirements:
        - Files must contain the required columns for their respective data types
        - Timestamps should be in a recognizable format
        - Text fields should be properly encoded
        """)

if __name__ == "__main__":
    main()
