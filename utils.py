import pandas as pd
import logging
from typing import Optional, Tuple
from pathlib import Path

def validate_file_type(file_path: Path) -> bool:
    """Validate if file type is supported"""
    return file_path.suffix.lower() in ['.csv', '.xlsx', '.xls']

def read_file_sample(file_path: Path, sample_size: int = 5) -> Tuple[Optional[pd.DataFrame], str]:
    """Read a sample of rows from file for preview"""
    try:
        if file_path.suffix.lower() == '.csv':
            df = pd.read_csv(file_path, nrows=sample_size)
            return df, 'csv'
        else:
            df = pd.read_excel(file_path, nrows=sample_size)
            return df, 'excel'
    except Exception as e:
        logging.error(f"Error reading file sample: {e}")
        return None, ''

def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Enhanced dataframe cleaning and validation"""
    # Remove completely empty rows and columns
    df = df.dropna(how='all')
    df = df.dropna(axis=1, how='all')
    
    # Clean string columns
    for col in df.select_dtypes(include=['object']).columns:
        # Strip whitespace
        df[col] = df[col].str.strip()
        # Replace empty strings with NaN
        df[col] = df[col].replace(r'^\s*$', pd.NA, regex=True)
        # Remove special characters that might cause JSON issues
        df[col] = df[col].apply(lambda x: x.encode('ascii', 'ignore').decode('ascii') if isinstance(x, str) else x)
    
    # Handle datetime columns
    datetime_cols = df.select_dtypes(include=['datetime64']).columns
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Handle numeric columns
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Drop rows where all key columns are NA
    df = df.dropna(subset=df.columns, how='all')
    
    return df
