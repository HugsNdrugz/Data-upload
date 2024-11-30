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
    """Basic dataframe cleaning"""
    # Remove completely empty rows and columns
    df = df.dropna(how='all')
    df = df.dropna(axis=1, how='all')
    
    # Strip whitespace from string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()
    
    return df
