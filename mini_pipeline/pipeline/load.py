# Load module
def write_csv(df, file_path):
    """Write DataFrame to CSV file"""
    df.to_csv(file_path, index=False)

# You can add more load functions later (e.g., to_excel, to_db, etc.)