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
    layout="centered",
    initial_sidebar_state="collapsed"
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
    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=['csv', 'xlsx', 'xls'], key='file_uploader')
    
    # Add a session state to track upload status
    if 'upload_state' not in st.session_state:
        st.session_state.upload_state = False
    
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
            
            # Display basic statistics in a mobile-friendly layout
            st.write("#### Dataset Overview")
            metrics_container = st.container()
            with metrics_container:
                # Use smaller columns on mobile
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Rows", len(df_preview))
                    st.metric("Total Columns", len(df_preview.columns))
                with col2:
                    st.metric("Memory Usage", f"{df_preview.memory_usage(deep=True).sum() / 1024:.2f} KB")
                    null_count = df_preview.isnull().sum().sum()
                    st.metric("Missing Values", null_count)
            
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
            
            # Mobile-friendly data preview with tabs
            tab1, tab2 = st.tabs(["Data Preview", "Column Info"])
            
            with tab1:
                st.write("#### Data Preview (First 50 rows)")
                st.dataframe(
                    df_preview.head(50),
                    use_container_width=True,
                    height=400
                )
            
            with tab2:
                st.write("#### Column Information")
                col_info = pd.DataFrame({
                    'Column': df_preview.columns,
                    'Type': df_preview.dtypes,
                    'Non-Null': df_preview.count(),
                    'Missing': df_preview.isnull().sum()
                })
                st.dataframe(
                    col_info,
                    use_container_width=True,
                    height=400
                )

            # Process button
            if st.button("Process and Import Data", key='process_button'):
                st.session_state.upload_state = True
            
            if st.session_state.upload_state:
                progress_bar = st.progress(0)
                status_container = st.empty()
                
                with st.spinner("Processing data..."):
                    # Save uploaded file to temp location
                    temp_path = save_uploaded_file(uploaded_file)
                    if temp_path:
                        try:
                            # Update progress
                            status_container.info("Validating data format...")
                            progress_bar.progress(25)
                            
                            # Process the file
                            result = process_and_insert_data(Path(temp_path))
                            
                            # Update progress
                            progress_bar.progress(100)
                            status_container.success("Data imported successfully!")
                            
                            # Display detailed import statistics
                            if result:
                                st.write("### Import Statistics")
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.metric("Total Rows", result["total_rows"])
                                    st.metric("Processed Rows", result["processed_rows"])
                                with col2:
                                    st.metric("Failed Rows", result["failed_rows"])
                                    st.metric("Target Table", result["table_name"])
                                
                                if result["failed_rows"] > 0:
                                    st.warning(f"⚠️ {result['failed_rows']} rows failed to import. Please check the data format.")
                                
                        except ValueError as ve:
                            progress_bar.progress(100)
                            status_container.error(f"Validation Error: {str(ve)}")
                            st.error("Please ensure your data matches the required format and try again.")
                        except Exception as e:
                            progress_bar.progress(100)
                            status_container.error(f"Error processing data: {str(e)}")
                            st.error("An unexpected error occurred. Please check the data format and try again.")
                        finally:
                            # Cleanup temp file
                            os.unlink(temp_path)
                    else:
                        progress_bar.progress(100)
                        status_container.error("Failed to save uploaded file")
                        st.error("Could not process the uploaded file. Please try again.")

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
