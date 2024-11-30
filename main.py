import streamlit as st
import pandas as pd
import os
from pathlib import Path
from data_processor import process_and_insert_data
from utils import validate_file_type, read_file_sample, sanitize_dataframe

st.set_page_config(
    page_title="Data Import Tool",
    page_icon="📊",
    initial_sidebar_state="collapsed"  # Better for mobile
)

# Enhanced mobile-first CSS
st.markdown("""
    <style>
        /* Mobile-optimized button styles */
        .stButton > button {
            width: 100%;
            min-height: 3.5rem;
            margin: 0.75rem 0;
            border-radius: 10px;
            font-size: 1.1rem;
            touch-action: manipulation;
        }
        
        /* Container spacing for mobile */
        .block-container {
            padding: 1.5rem 1rem;
            max-width: 100%;
        }
        
        /* Enhanced table display for mobile */
        .stDataFrame {
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
            margin: 1rem -1rem;
            padding: 0 1rem;
        }
        
        /* Mobile-friendly spacing and text */
        .element-container {
            margin-bottom: 1.5rem;
        }
        
        /* Improve readability on mobile */
        .streamlit-expanderHeader {
            font-size: 1.1rem;
            padding: 1rem 0.75rem;
        }
        
        /* Better touch targets */
        .streamlit-expanderContent {
            padding: 1rem;
        }
        
        /* Enhanced mobile metrics display */
        [data-testid="stMetricValue"] {
            font-size: 1.5rem !important;
        }
        
        /* Mobile-optimized headers */
        h1, h2, h3 {
            margin: 1rem 0;
            line-height: 1.4;
        }
        
        /* Improve form elements for touch */
        .stSelectbox, .stTextInput {
            margin: 1rem 0;
        }
        
        /* Better error message display */
        .stAlert {
            padding: 1rem;
            border-radius: 10px;
            margin: 1rem 0;
        }
        
        /* Loading indicator optimization */
        .stProgress > div > div {
            height: 10px;
            border-radius: 5px;
        }
        
        /* Responsive images */
        img {
            max-width: 100%;
            height: auto;
        }
        
        /* Mobile-friendly JSON display */
        .json-object {
            font-size: 0.9rem;
            padding: 0.75rem;
            border-radius: 8px;
            overflow-x: auto;
        }
    </style>
""", unsafe_allow_html=True)

def save_uploaded_file(uploaded_file):
    """Save uploaded file to temp location"""
    try:
        # Create a temporary file path
        temp_dir = Path("temp")
        temp_dir.mkdir(exist_ok=True)
        temp_path = temp_dir / uploaded_file.name
        
        # Save uploaded file
        with open(temp_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        return str(temp_path)
    except Exception as e:
        st.error(f"Error saving file: {e}")
        return None

def main():
    st.title("Data Import Tool")
    
    # Initialize session state
    if 'upload_state' not in st.session_state:
        st.session_state.upload_state = False
    
    # File uploader
    uploaded_file = st.file_uploader(
        "Choose a CSV or Excel file",
        type=['csv', 'xlsx', 'xls'],
        help="Upload your data file here. Supported formats: CSV, Excel (xlsx, xls)"
    )
    
    if uploaded_file is not None:
        try:
            # File information
            st.write("### File Information")
            file_info = {
                "Filename": str(uploaded_file.name),
                "Size": f"{uploaded_file.size / 1024:.2f} KB",
                "Type": str(uploaded_file.type)
            }
            st.json(file_info)

            # Preview data with error handling
            try:
                if uploaded_file.type == "text/csv":
                    df_preview = pd.read_csv(uploaded_file)
                elif any(uploaded_file.name.lower().endswith(ext) for ext in ['.xlsx', '.xls']):
                    # Save the file temporarily
                    temp_path = Path("temp") / uploaded_file.name
                    temp_path.parent.mkdir(exist_ok=True)
                    
                    with open(temp_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    
                    try:
                        # Read Excel file treating first row as column names
                        df_preview = pd.read_excel(
                            temp_path,
                            engine='openpyxl' if uploaded_file.name.lower().endswith('.xlsx') else 'xlrd',
                            header=0  # Explicitly set to use first row as headers
                        )
                        
                        # If the first row contains metadata about tracking smartphone
                        if not df_preview.empty and any(df_preview.columns.astype(str).str.contains('Tracking Smartphone', case=False, na=False)):
                            # Read again with the second row as headers
                            df_preview = pd.read_excel(
                                temp_path,
                                engine='openpyxl' if uploaded_file.name.lower().endswith('.xlsx') else 'xlrd',
                                header=1  # Use second row as headers since first row is metadata
                            )
                    except Exception as e:
                        st.error(f"Failed to read Excel file: {str(e)}")
                        return
                    finally:
                        # Clean up temp file
                        if temp_path.exists():
                            temp_path.unlink()
                else:
                    st.error("Unsupported file type")
                    return
                
                # Clean the preview data
                df_preview = sanitize_dataframe(df_preview)
                
                # Basic Statistics
                st.write("### Data Analysis")
                
                # Data Quality Metrics
                st.write("#### Data Quality Overview")
                # Use 2 columns for better mobile layout
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Rows", len(df_preview))
                    st.metric("Total Columns", len(df_preview.columns))
                    st.metric("Missing Values", df_preview.isnull().sum().sum())
                with col2:
                    st.metric("Duplicate Rows", len(df_preview) - len(df_preview.drop_duplicates()))
                    non_null_counts = df_preview.count()
                    completeness = (non_null_counts / len(df_preview)).mean() * 100
                    st.metric("Data Completeness", f"{completeness:.1f}%")
                    st.metric("Size", f"{df_preview.memory_usage(deep=True).sum() / 1024:.1f} KB")
                
                # Column Analysis
                st.write("#### Column Analysis")
                column_analysis = pd.DataFrame({
                    'Column': df_preview.columns,
                    'Type': df_preview.dtypes,
                    'Non-Null Count': df_preview.count(),
                    'Null Count': df_preview.isnull().sum(),
                    'Unique Values': df_preview.nunique(),
                })
                st.dataframe(column_analysis, use_container_width=True)
                
                # Date columns analysis
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
                
                # Enhanced mobile-friendly data preview
                with st.expander("📊 Data Preview", expanded=True):
                    # Tab-based navigation for better mobile experience
                    preview_tab, stats_tab = st.tabs(["Preview Data", "Column Info"])
                    
                    with preview_tab:
                        # Compact preview with horizontal scroll
                        st.markdown("""
                            <style>
                                .preview-container { overflow-x: auto; -webkit-overflow-scrolling: touch; }
                            </style>
                        """, unsafe_allow_html=True)
                        
                        # Show simplified preview for mobile
                        preview_height = 250 if len(df_preview.columns) > 4 else 400
                        st.dataframe(
                            df_preview.head(10),  # Show fewer rows for better mobile view
                            use_container_width=True,
                            height=preview_height
                        )
                        
                        # Add swipe hint for mobile
                        st.caption("👈 Swipe left/right to view all columns")
                    
                    with stats_tab:
                        # Enhanced column information display
                        col_info = pd.DataFrame({
                            'Column': df_preview.columns,
                            'Type': df_preview.dtypes.astype(str),
                            'Missing (%)': (df_preview.isnull().sum() / len(df_preview) * 100).round(1)
                        })
                        
                        # Display stats in a mobile-friendly format
                        for _, row in col_info.iterrows():
                            with st.container():
                                st.markdown(f"""
                                    <div style='padding: 0.5rem; background-color: #f0f2f6; border-radius: 8px; margin-bottom: 0.5rem;'>
                                        <strong>{row['Column']}</strong><br/>
                                        Type: {row['Type']}<br/>
                                        Missing: {row['Missing (%)']}%
                                    </div>
                                """, unsafe_allow_html=True)

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
                                
                                # Clean and validate the data before processing
                                if uploaded_file.type == "text/csv":
                                    df = pd.read_csv(temp_path)
                                else:
                                    # Read Excel with proper header handling
                                    file_path = Path(temp_path)
                                    df = pd.read_excel(
                                        temp_path,
                                        engine='openpyxl' if str(file_path).lower().endswith('.xlsx') else 'xlrd',
                                        header=0
                                    )
                                    # Check if first row is metadata and reread with correct header
                                    if any(df.columns.astype(str).str.contains('Tracking Smartphone', case=False, na=False)):
                                        df = pd.read_excel(
                                            temp_path,
                                            engine='openpyxl' if str(file_path).lower().endswith('.xlsx') else 'xlrd',
                                            header=1
                                        )
                                
                                # Apply data cleaning
                                df = sanitize_dataframe(df)
                                
                                # Show preview of cleaned data
                                st.write("### Preview of Cleaned Data")
                                st.dataframe(df.head())
                                
                                # Process the cleaned file
                                result = process_and_insert_data(Path(temp_path))
                                
                                # Update progress
                                progress_bar.progress(100)
                                status_container.success("Data imported successfully!")
                                
                                # Display import statistics
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
                                status_container.error("⚠️ Validation Error")
                                # Enhanced mobile-friendly error display
                                st.markdown(f"""
                                    <div style='
                                        padding: 1.2rem;
                                        background-color: #ffebe6;
                                        border-radius: 12px;
                                        margin: 1rem -0.5rem;
                                        box-shadow: 0 2px 4px rgba(255,75,75,0.1);
                                    '>
                                        <div style='
                                            display: flex;
                                            align-items: center;
                                            margin-bottom: 0.8rem;
                                        '>
                                            <span style='
                                                font-size: 1.5rem;
                                                margin-right: 0.5rem;
                                            '>⚠️</span>
                                            <h4 style='
                                                margin: 0;
                                                color: #ff4b4b;
                                                font-size: 1.1rem;
                                            '>Error Details</h4>
                                        </div>
                                        <p style='
                                            margin: 0.8rem 0;
                                            font-size: 1rem;
                                            line-height: 1.5;
                                            color: #484848;
                                        '>{str(ve)}</p>
                                        <div style='
                                            margin-top: 1rem;
                                            padding-top: 0.8rem;
                                            border-top: 1px solid rgba(255,75,75,0.2);
                                        '>
                                            <p style='
                                                margin: 0;
                                                font-size: 0.95rem;
                                                color: #666;
                                            '>💡 Try This:
                                            <br/>• Check your data format
                                            <br/>• Ensure all required fields are present
                                            <br/>• Verify the file is not corrupted</p>
                                        </div>
                                    </div>
                                """, unsafe_allow_html=True)
                            except Exception as e:
                                progress_bar.progress(100)
                                status_container.error(f"Error processing data: {str(e)}")
                                st.error("An unexpected error occurred. Please check the data format and try again.")
                            finally:
                                # Cleanup temp file
                                if os.path.exists(temp_path):
                                    os.unlink(temp_path)
                        else:
                            progress_bar.progress(100)
                            status_container.error("Failed to save uploaded file")
                            st.error("Could not process the uploaded file. Please try again.")
            
            except Exception as e:
                st.error(f"Error reading file: {e}")
                st.error("Please ensure your file is not corrupted and try again.")
                
        except Exception as e:
            st.error(f"Error processing file: {e}")
    
    # Mobile-friendly Help section with icons
    with st.expander("📱 Help & Instructions"):
        st.markdown("""
        <div style='padding: 1rem; background-color: #f0f2f6; border-radius: 10px;'>
            <h3 style='margin-top: 0;'>📝 Quick Guide:</h3>
            <ol style='margin-left: 1rem;'>
                <li style='margin: 0.5rem 0;'>📂 Upload your CSV or Excel file</li>
                <li style='margin: 0.5rem 0;'>👀 Check the preview</li>
                <li style='margin: 0.5rem 0;'>▶️ Tap 'Process' to start import</li>
            </ol>
            
            <h4>📁 Supported Files:</h4>
            <ul style='margin-left: 1rem;'>
                <li style='margin: 0.5rem 0;'>CSV (.csv)</li>
                <li style='margin: 0.5rem 0;'>Excel (.xlsx, .xls)</li>
            </ul>
            
            <h4>⚠️ Requirements:</h4>
            <ul style='margin-left: 1rem;'>
                <li style='margin: 0.5rem 0;'>Required columns must be present</li>
                <li style='margin: 0.5rem 0;'>Valid timestamp formats</li>
                <li style='margin: 0.5rem 0;'>Proper text encoding</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
