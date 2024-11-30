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
    return col.lower().strip().replace(' ', '_')

def identify_table(df: pd.DataFrame) -> Optional[str]:
    """Identifies the table based on the file's headers with flexible matching."""
    file_headers = {normalize_column_name(col) for col in df.columns}
    logging.info(f"Found headers in file: {file_headers}")
    
    for table, schema in TABLE_SCHEMAS.items():
        schema_headers = {normalize_column_name(col) for col in schema["columns"]}
        rename_headers = {normalize_column_name(col) for col in schema["renames"].keys()}
        
        logging.info(f"Checking table {table}")
        logging.info(f"Schema headers: {schema_headers}")
        logging.info(f"Rename headers: {rename_headers}")
        
        # Check for Keylog table first
        if table in ['KeylogImport', 'Keylogs'] and set(['application', 'time', 'text']).issubset(file_headers):
            logging.info(f"Matched {table} table")
            return table
            
        # Then check other tables
        if schema_headers.issubset(file_headers) or rename_headers.issubset(file_headers):
            logging.info(f"Matched {table} table")
            return table
    
    logging.error("No matching table schema found")
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

def parse_timestamp_flexible(date_str: str, timezone: str = "UTC") -> Optional[datetime]:
    """Parse timestamp with timezone handling"""
    if pd.isna(date_str) or not date_str:
        return None
    
    try:
        # Handle string dates
        if isinstance(date_str, str):
            try:
                dt = parse(date_str)
                if dt.tzinfo is None:
                    dt = pytz.timezone(timezone).localize(dt)
                return dt
            except:
                return None
        return None
    except Exception as e:
        logging.warning(f"Failed to parse timestamp '{date_str}': {e}")
        return None

def validate_data(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Validates and cleans data according to schema"""
    schema = TABLE_SCHEMAS.get(table_name)
    if not schema:
        raise ValueError(f"Schema not found for table: {table_name}")
    
    # Normalize column names
    df.columns = [normalize_column_name(col) for col in df.columns]
    
    # Map columns to schema
    column_mapping = {}
    for expected_col in schema["columns"]:
        normalized_expected = normalize_column_name(expected_col)
        if normalized_expected in df.columns:
            column_mapping[normalized_expected] = expected_col
    
    if not column_mapping:
        raise ValueError("No matching columns found in the data")
    
    # Select and rename columns
    df = df.rename(columns=column_mapping)
    
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
    
    return df[list(column_mapping.values())]

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
        # Read file
        if file_path.suffix.lower() == '.csv':
            df = pd.read_csv(file_path)
            logging.info("Successfully read CSV file")
        else:
            logging.info("Attempting to read Excel file")
            try:
                df = pd.read_excel(file_path, engine='openpyxl')
                # Remove the first row after reading
                df = df.iloc[1:]
                df = df.reset_index(drop=True)
                logging.info("Successfully read Excel file with openpyxl and removed first row")
            except Exception as e:
                logging.warning(f"Failed to read with openpyxl: {e}")
                try:
                    df = pd.read_excel(file_path, engine='xlrd')
                    # Remove the first row after reading
                    df = df.iloc[1:]
                    df = df.reset_index(drop=True)
                    logging.info("Successfully read Excel file with xlrd and removed first row")
                except Exception as e:
                    logging.error(f"Failed to read with xlrd: {e}")
                    raise ValueError(f"Could not read Excel file: {e}")
        
        stats["total_rows"] = len(df)
        
        # Identify table
        table_name = identify_table(df)
        if not table_name:
            raise ValueError("Could not identify table schema")
        
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
    # Add any additional initialization here if needed