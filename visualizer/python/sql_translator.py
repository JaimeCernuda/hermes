import sqlite3
import pandas as pd


def get_latest_entries(db_path, table_name, n):
    conn = sqlite3.connect(db_path)
    query = f"SELECT * FROM {table_name} ORDER BY rowid DESC LIMIT {n};"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


if __name__ == "__main__":
    db_path = "path_to_your_database.db"  # Replace with the path to your SQLite database
    n = 5  # Number of latest entries to retrieve

    # Apps Table
    apps_latest_entries = get_latest_entries(db_path, "Apps", n)
    print("Apps Table - Latest Entries:")
    print(apps_latest_entries)
    print("\n")

    # BlobLocations Table
    blob_locations_latest_entries = get_latest_entries(db_path, "BlobLocations", n)
    print("BlobLocations Table - Latest Entries:")
    print(blob_locations_latest_entries)
    print("\n")

    # VariableMetadataTable
    variable_metadata_latest_entries = get_latest_entries(db_path, "VariableMetadataTable", n)
    print("VariableMetadataTable - Latest Entries:")
    print(variable_metadata_latest_entries)
