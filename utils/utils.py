from datetime import datetime

def current_time() -> datetime:
    """
    Returns the fixed simulated current time for the dataset (2025-11-14 23:18:00).
    This is used because the dataset tables are from 2025.
    
    Returns:
        datetime: A datetime object set to 2025-11-14 23:18:00
    """
    return datetime(2025, 11, 14, 23, 18, 0)
