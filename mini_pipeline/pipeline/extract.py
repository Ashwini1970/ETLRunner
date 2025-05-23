# Extract module
import pandas as pd

def read_csv(file_path):
    """Read CSV file and return a DataFrame"""
    return pd.read_csv(file_path)

# You can add more extract functions later (e.g., read_excel, from_db, etc.)