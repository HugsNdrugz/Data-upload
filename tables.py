import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path
import re
from tkinter import Tk, filedialog
from typing import Optional, List, Dict, Any
import logging
from dateutil.parser import parse
import pytz  # For timezone handling

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration (can be moved to a config file)
DATABASE_FILE = "data.db"
DEFAULT_TIMEZONE = "UTC"
BATCH_SIZE = 1000  # Number of rows to insert at a time

# Correct table headers with data types for validation
TABLE_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "Contacts": {
        "columns": ["name", "phone_number", "email", "last_contacted"],
        "types": [str, str, str, datetime],
        "renames": {"Name": "name", "Phone Number": "phone_number", "Email Id": "email", "Last Contacted": "last_contacted"}
    },
    "InstalledApps": {
        "columns": ["application_name", "package_name", "install_date"],
        "types": [str, str, datetime],
        "renames": {"Application Name": "application_name", "Package Name": "package_name", "Installed Date": "install_date"}
    },
    "Calls": {
        "columns": ["call_type", "time", "from_to", "duration", "location"],
        "types": [str, datetime, str, int, str],
        "renames": {"Call type": "call_type", "Time": "time", "From/To": "from_to", "Duration (Sec)": "duration", "Location": "location"}
    },
    "SMS": {
        "columns": ["sms_type", "time", "from_to", "text", "location"],
        "types": [str, datetime, str, str, str],
        "renames": {"SMS type": "sms_type", "Time": "time", "From/To": "from_to", "Text": "text", "Location": "location"}
    },
    "ChatMessages": {
        "columns": ["messenger", "time", "sender", "text"],
        "types": [str, datetime, str, str],
        "renames": {"Messenger": "messenger", "Time": "time", "Sender": "sender", "Text": "text"}
    },
    "Keylogs": {
        "columns": ["application", "time", "text"],
        "types": [str, datetime, str],
        "renames": {"Application": "application", "Time": "time", "Text": "text"}
    },
}

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
        package_name TEXT NOT NULL,
        install_date DATETIME,
        UNIQUE(package_name, install_date)
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
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            cursor.executescript(schema)
            conn.commit()
        logging.info("Database initialized successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error initializing database: {e}")

def parse_timestamp_flexible(date_str: str, timezone: str = DEFAULT_TIMEZONE) -> Optional[datetime]:
    """Parses a date string into a datetime object, handling various formats and timezones."""
    try:
        dt = parse(date_str)
        if dt.tzinfo is None:
            tz = pytz.timezone(timezone)
            dt = tz.localize(dt)
        else:
            dt = dt.astimezone(pytz.timezone(timezone))
        return dt
    except (ValueError, TypeError) as e:
        logging.warning(f"Failed to parse timestamp '{date_str}': {e}")
        return None

def validate_data(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Validates data types and columns against the table schema."""
    schema = TABLE_SCHEMAS.get(table_name)
    if not schema:
        raise ValueError(f"Schema not found for table: {table_name}")
    
    expected_columns = schema["columns"]
    expected_types = schema["types"]

    # Check if all expected columns exist
    missing_columns = set(expected_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns for table {table_name}: {', '.join(missing_columns)}")

    # Ensure DataFrame only contains the expected columns
    df = df[expected_columns]

    # Validate data types
    for col, dtype in zip(expected_columns, expected_types):
        try:
            if dtype == datetime:
                df[col] = df[col].apply(lambda x: parse_timestamp_flexible(x) if pd.notna(x) and isinstance(x, str) else x)
                # Additional check to ensure conversion was successful
                failed_conversions = df[col].isnull().sum()
                if failed_conversions > 0:
                   logging.warning(f"Failed to convert {failed_conversions} values in column '{col}' to datetime for table {table_name}.")
            elif dtype == int:
                    # Ensure that numeric values are converted safely
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            else:
                df[col] = df[col].astype(dtype)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Type conversion error in column '{col}' for table '{table_name}': {e}")

    return df

def process_data(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Processes the data for the given table."""
    try:
        schema = TABLE_SCHEMAS.get(table_name)
        if not schema:
            raise ValueError(f"Schema not found for table: {table_name}")

        # Remove duplicates for InstalledApps based on package_name and install_date
        if table_name == "InstalledApps":
            df = df.drop_duplicates(subset=["package_name", "install_date"])

        df = df.rename(columns=schema["renames"])
        df = validate_data(df, table_name)
    except Exception as e:
        logging.error(f"Error processing data for {table_name}: {e}")
        raise
    return df

def identify_table(df: pd.DataFrame) -> Optional[str]:
    """Identifies the table based on the file's headers."""
    file_headers = set(df.columns)
    for table, schema in TABLE_SCHEMAS.items():
        if set(schema["renames"].keys()).issubset(file_headers):
            return table
        elif set(schema["columns"]).issubset(file_headers):
            return table
    return None

def insert_data(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame) -> int:
    """Inserts data into the database in batches, handling unique constraints."""
    try:
        total_rows = len(df)
        successful_inserts = 0
        
        for i in range(0, total_rows, BATCH_SIZE):
            batch_df = df.iloc[i:i + BATCH_SIZE]
            
            if table_name == "InstalledApps":
                # For InstalledApps, handle each row individually with INSERT OR REPLACE
                cursor = conn.cursor()
                for _, row in batch_df.iterrows():
                    try:
                        cursor.execute("""
                            INSERT OR REPLACE INTO InstalledApps 
                            (application_name, package_name, install_date)
                            VALUES (?, ?, ?)
                        """, (row['application_name'], row['package_name'], row['install_date']))
                        successful_inserts += 1
                    except sqlite3.Error as e:
                        logging.warning(f"Could not process row with package_name {row['package_name']}: {e}")
                conn.commit()
            else:
                # For other tables, use the standard batch insert
                batch_df.to_sql(table_name, conn, if_exists="append", index=False)
                successful_inserts += len(batch_df)
            
            logging.info(f"Processed rows {i} to {min(i + BATCH_SIZE, total_rows)} for {table_name}.")
        
        logging.info(f"Successfully processed {successful_inserts} out of {total_rows} rows for {table_name}.")
        return successful_inserts
        
    except Exception as e:
        logging.error(f"Unexpected error during data insertion into {table_name}: {e}")
        raise

def process_and_insert_data(file_path: Path) -> Dict[str, int]:
    """Processes data and inserts it into the database in batches."""
    try:
        # Dynamically handle CSV or Excel
        if file_path.suffix.lower() == ".csv":
            df = pd.read_csv(file_path)
            logging.info(f"Successfully read CSV file: {file_path}")
        elif file_path.suffix.lower() in [".xls", ".xlsx"]:
            df = pd.read_excel(file_path)
            logging.info(f"Successfully read Excel file: {file_path}")
        else:
            raise ValueError("Unsupported file type.")

        table_name = identify_table(df)
        if not table_name:
            raise ValueError("Could not identify the table based on the file headers.")
        
        logging.info(f"Identified table: {table_name}")
        df = process_data(table_name, df)

        with sqlite3.connect(DATABASE_FILE) as conn:
            processed_rows = insert_data(conn, table_name, df)
            
        return {
            "table_name": table_name,
            "total_rows": len(df),
            "processed_rows": processed_rows,
            "failed_rows": len(df) - processed_rows
        }

    except ValueError as ve:
        logging.error(f"Data processing error: {ve}")
        raise
    except pd.errors.EmptyDataError:
        logging.error(f"Error: The file {file_path} is empty or contains no data.")
        raise
    except pd.errors.ParserError:
        logging.error(f"Error: Unable to parse the file {file_path}. It may be corrupted or in an unexpected format.")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during file processing: {e}")
        raise

def select_file() -> Optional[Path]:
    """Opens a file dialog to select a file and returns the file path."""
    root = Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv"), ("Excel Files", "*.xls;*.xlsx")])
    return Path(file_path) if file_path else None

def main():
    init_db()
    file_path = select_file()
    if not file_path:
        logging.info("No file selected. Exiting.")
        return
    process_and_insert_data(file_path)
    logging.info("Data processing complete.")

if __name__ == "__main__":
    main()
