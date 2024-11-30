import pandas as pd
import sqlite3
from pathlib import Path
import logging
from datetime import datetime
import pytz
from dateutil.parser import parse
from typing import Optional, Dict, Any
from schemas import TABLE_SCHEMAS, DATABASE_FILE, BATCH_SIZE

def normalize_column_name(col: str) -> str:
    """Normalize column name for comparison"""
    return str(col).lower().strip().replace(' ', '_')

def identify_table(df: pd.DataFrame) -> Optional[str]:
    """Identifies the table based on the file's headers with flexible matching."""
    # Convert all headers to normalized form for comparison
    file_headers = {normalize_column_name(col) for col in df.columns}
    logging.info(f"Found headers in file: {list(df.columns)}")
    logging.info(f"Normalized headers: {list(file_headers)}")
    
    for table, schema in TABLE_SCHEMAS.items():
        schema_headers = {normalize_column_name(col) for col in schema["columns"]}
        rename_headers = {normalize_column_name(col) for col in schema["renames"].keys()}
        
        logging.info(f"Checking table {table}")
        logging.info(f"Schema headers: {schema_headers}")
        logging.info(f"Rename headers: {rename_headers}")
        
        # Special handling for Keylog tables
        if table in ['KeylogImport', 'Keylogs']:
            required_keylog_headers = {'application', 'time', 'text'}
            normalized_required = {normalize_column_name(h) for h in required_keylog_headers}
            if normalized_required.issubset(file_headers):
                logging.info(f"Matched {table} table based on required keylog headers")
                return table
        
        # Check if either schema headers or rename headers are present
        # Make the matching more lenient by checking if most headers are present
        schema_match_ratio = len(schema_headers.intersection(file_headers)) / len(schema_headers)
        rename_match_ratio = len(rename_headers.intersection(file_headers)) / len(rename_headers)
        
        # If 80% or more headers match, consider it a match
        if schema_match_ratio >= 0.8 or rename_match_ratio >= 0.8:
            logging.info(f"Matched {table} table with match ratio: {max(schema_match_ratio, rename_match_ratio):.2f}")
            return table
    
    logging.error("No matching table schema found")
    logging.error(f"Available headers: {list(df.columns)}")
    return None

def init_db():
    """Initializes the SQLite database and creates tables if they don't exist."""
    schema = """
    CREATE TABLE IF NOT EXISTS Contacts (
        contact_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        phone_number TEXT,
        email TEXT,
        last_contacted DATETIME
    );

    CREATE TABLE IF NOT EXISTS InstalledApps (
        app_id INTEGER PRIMARY KEY AUTOINCREMENT,
        application_name TEXT NOT NULL,
        package_name TEXT UNIQUE NOT NULL,
        install_date DATETIME
    );

    CREATE TABLE IF NOT EXISTS Calls (
        call_id INTEGER PRIMARY KEY AUTOINCREMENT,
        call_type TEXT NOT NULL,
        time DATETIME NOT NULL,
        from_to TEXT,
        duration INTEGER DEFAULT 0,
        location TEXT
    );

    CREATE TABLE IF NOT EXISTS SMS (
        sms_id INTEGER PRIMARY KEY AUTOINCREMENT,
        sms_type TEXT NOT NULL,
        time DATETIME NOT NULL,
        from_to TEXT,
        text TEXT,
        location TEXT
    );

    CREATE TABLE IF NOT EXISTS ChatMessages (
        message_id INTEGER PRIMARY KEY AUTOINCREMENT,
        messenger TEXT NOT NULL,
        time DATETIME NOT NULL,
        sender TEXT,
        text TEXT
    );

    CREATE TABLE IF NOT EXISTS Keylogs (
        keylog_id INTEGER PRIMARY KEY AUTOINCREMENT,
        application TEXT NOT NULL,
        time DATETIME NOT NULL,
        text TEXT
    );
    """
    with sqlite3.connect(DATABASE_FILE) as conn:
        conn.executescript(schema)
        logging.info("Database initialized successfully")

def parse_timestamp_flexible(date_str: str, timezone: str = "UTC") -> Optional[datetime]:
    """Parse timestamp with flexible format handling"""
    if pd.isna(date_str) or not date_str:
        return None
    
    try:
        if isinstance(date_str, str):
            dt = parse(date_str)
            if dt.tzinfo is None:
                dt = pytz.timezone(timezone).localize(dt)
            return dt
        return date_str if isinstance(date_str, datetime) else None
    except Exception as e:
        logging.warning(f"Failed to parse timestamp '{date_str}': {e}")
        return None

def validate_data(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Validates and cleans data according to schema"""
    schema = TABLE_SCHEMAS.get(table_name)
    if not schema:
        raise ValueError(f"Schema not found for table: {table_name}")
    
    # Store original column names for reference
    original_columns = df.columns
    logging.info(f"Original columns: {list(original_columns)}")
    
    # Create a mapping of normalized names to original names
    norm_to_orig = {normalize_column_name(col): col for col in original_columns}
    
    # Map schema columns to actual columns
    column_mapping = {}
    for expected_col in schema["columns"]:
        norm_expected = normalize_column_name(expected_col)
        if norm_expected in norm_to_orig:
            column_mapping[norm_to_orig[norm_expected]] = expected_col
    
    if not column_mapping:
        raise ValueError(f"No matching columns found in the data. Expected columns: {schema['columns']}")
    
    # Select and rename columns
    df = df[list(column_mapping.keys())].rename(columns=column_mapping)
    
    # Apply data type conversions
    for col, dtype in zip(schema["columns"], schema["types"]):
        if col in df.columns:
            try:
                if dtype == datetime:
                    df[col] = df[col].apply(parse_timestamp_flexible)
                elif dtype == int:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                else:
                    df[col] = df[col].astype(str)
            except Exception as e:
                logging.warning(f"Error converting column {col}: {e}")
    
    return df

def process_and_insert_data(file_path: Path) -> Dict[str, Any]:
    """Process and import data with statistics tracking"""
    stats = {
        "total_rows": 0,
        "processed_rows": 0,
        "failed_rows": 0,
        "table_name": None
    }
    
    try:
        logging.info(f"Processing file: {file_path}")
        
        # Read the file
        if file_path.suffix.lower() == '.csv':
            df = pd.read_csv(file_path)
        else:
            try:
                df = pd.read_excel(file_path, engine='openpyxl')
            except Exception as e:
                logging.warning(f"Failed to read with openpyxl: {e}")
                df = pd.read_excel(file_path, engine='xlrd')
        
        stats["total_rows"] = len(df)
        
        # Identify table
        table_name = identify_table(df)
        if not table_name:
            raise ValueError("Could not identify table schema. Please check the file headers.")
        
        stats["table_name"] = table_name
        
        # Process data
        df = validate_data(df, table_name)
        
        # Insert data
        with sqlite3.connect(DATABASE_FILE) as conn:
            for i in range(0, len(df), BATCH_SIZE):
                batch = df.iloc[i:i + BATCH_SIZE]
                batch.to_sql(table_name, conn, if_exists='append', index=False)
                stats["processed_rows"] += len(batch)
        
        return stats
        
    except Exception as e:
        stats["failed_rows"] = stats["total_rows"] - stats["processed_rows"]
        logging.error(f"Error processing file: {e}")
        raise

def main():
    init_db()
