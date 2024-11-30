import pandas as pd
import sqlite3
from pathlib import Path
import logging
from datetime import datetime
import pytz
from dateutil.parser import parse
from typing import Optional, Dict, Any
from schemas import TABLE_SCHEMAS, DATABASE_FILE, BATCH_SIZE

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
        return datetime.now(pytz.timezone(timezone))  # Default to current time for required fields
    
    # Define supported date formats
    date_formats = [
        "%b %d, %I:%M %p",     # Jun 7, 1:28 PM
        "%Y-%m-%d %H:%M:%S",   # 2024-06-07 13:28:00
        "%b %d, %Y %I:%M %p",  # Jun 7, 2024 1:28 PM
        "%Y-%m-%d %H:%M",      # 2024-06-07 13:28
        "%m/%d/%Y %H:%M:%S",   # 06/07/2024 13:28:00
        "%m/%d/%Y %I:%M %p",   # 06/07/2024 1:28 PM
    ]
    
    try:
        # Convert to string and handle numeric timestamps
        if isinstance(date_str, (int, float)):
            try:
                return datetime.fromtimestamp(float(date_str), pytz.timezone(timezone))
            except ValueError:
                return datetime.now(pytz.timezone(timezone))
        
        # Handle string dates
        date_str = str(date_str).strip()
        
        # Try specific formats first
        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                # If year is not in the format, use current year
                if dt.year == 1900:
                    current_year = datetime.now().year
                    dt = dt.replace(year=current_year)
                return pytz.timezone(timezone).localize(dt)
            except ValueError:
                continue
        
        # Try parsing with dateutil.parser as fallback
        try:
            dt = parse(date_str)
        except:
            # Fallback for Excel date format
            try:
                dt = datetime(1899, 12, 30) + pd.Timedelta(days=float(date_str))
            except:
                return datetime.now(pytz.timezone(timezone))
        
        # Set timezone if not present
        if dt.tzinfo is None:
            dt = pytz.timezone(timezone).localize(dt)
        return dt
    except Exception as e:
        logging.warning(f"Failed to parse timestamp '{date_str}': {e}")
        return datetime.now(pytz.timezone(timezone))

def validate_data(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Validate and clean data according to schema"""
    schema = TABLE_SCHEMAS[table_name]
    
    # Validate columns
    required_columns = schema["columns"]
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Select only required columns
    df = df[required_columns]
    
    # Apply type conversions
    for col, dtype in zip(schema["columns"], schema["types"]):
        try:
            if dtype == datetime:
                df[col] = df[col].apply(lambda x: parse_timestamp_flexible(x, "UTC"))
                if df[col].isnull().any():
                    logging.warning(f"Found null values in datetime column '{col}'. Using current timestamp as default.")
            elif dtype == int:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            else:
                df[col] = df[col].astype(str)
        except Exception as e:
            raise ValueError(f"Error converting column '{col}': {e}")
    
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
        # Read file
        if file_path.suffix.lower() == '.csv':
            df = pd.read_csv(file_path)
        else:
            # Skip first row for Excel files as it contains metadata
            df = pd.read_excel(file_path, skiprows=1)
        
        stats["total_rows"] = len(df)
        
        # Identify table
        table_name = None
        for name, schema in TABLE_SCHEMAS.items():
            if set(schema["renames"].keys()).issubset(df.columns):
                table_name = name
                break
        
        if not table_name:
            raise ValueError("Could not identify table schema")
        
        stats["table_name"] = table_name
        
        # Process data
        df = df.rename(columns=TABLE_SCHEMAS[table_name]["renames"])
        df = validate_data(df, table_name)
        
        # Insert data in batches
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
