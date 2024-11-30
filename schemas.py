from datetime import datetime
from typing import Dict, Any

DATABASE_FILE = "data.db"
BATCH_SIZE = 1000

TABLE_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "Contacts": {
        "columns": ["name", "phone_number", "email", "last_contacted"],
        "types": [str, str, str, datetime],
        "renames": {
            "Name": "name",
            "Phone Number": "phone_number",
            "Email Id": "email",
            "Last Contacted": "last_contacted"
        }
    },
    "InstalledApps": {
        "columns": ["application_name", "package_name", "install_date"],
        "types": [str, str, datetime],
        "renames": {
            "Application Name": "application_name",
            "Package Name": "package_name",
            "Installed Date": "install_date"
        }
    },
    "Calls": {
        "columns": ["call_type", "time", "from_to", "duration", "location"],
        "types": [str, datetime, str, int, str],
        "renames": {
            "Call type": "call_type",
            "Time": "time",
            "From/To": "from_to",
            "Duration (Sec)": "duration",
            "Location": "location"
        }
    },
    "SMS": {
        "columns": ["sms_type", "time", "from_to", "text", "location"],
        "types": [str, datetime, str, str, str],
        "renames": {
            "SMS type": "sms_type",
            "Time": "time",
            "From/To": "from_to",
            "Text": "text",
            "Location": "location"
        }
    },
    "ChatMessages": {
        "columns": ["messenger", "time", "sender", "text"],
        "types": [str, datetime, str, str],
        "renames": {
            "Messenger": "messenger",
            "Time": "time",
            "Sender": "sender",
            "Text": "text"
        }
    },
    "Keylogs": {
        "columns": ["application", "time", "text"],
        "types": [str, datetime, str],
        "renames": {
            "Application": "application",
            "Time": "time",
            "Text": "text"
        }
    },
    "KeylogImport": {
        "columns": ["application", "time", "text"],
        "types": [str, datetime, str],
        "renames": {
            "Application": "application",
            "Time": "time",
            "Text": "text"
        }
    }
}
