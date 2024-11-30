import sqlite3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# SQLite database file
DATABASE_FILE = "data.db"

def create_database():
    """Creates the SQLite database and tables."""
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
    try:
        # Connect to the SQLite database
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            # Execute the schema script
            cursor.executescript(schema)
            conn.commit()
        logging.info(f"Database '{DATABASE_FILE}' created successfully with all tables.")
    except sqlite3.Error as e:
        logging.error(f"Error creating database: {e}")

if __name__ == "__main__":
    create_database()
    print("Script executed successfully!")