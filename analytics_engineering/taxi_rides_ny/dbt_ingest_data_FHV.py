# Import required libraries:
# - duckdb: in-process analytical database
# - requests: to download files from the internet
# - Path: to handle file system paths in an OS-independent way
import duckdb
import requests
from pathlib import Path

# Base URL where the NYC taxi datasets are hosted (GitHub releases)
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"


def download_and_convert_files(taxi_type):
    """
    This function:
    1. Downloads compressed CSV (.csv.gz) taxi data files
    2. Converts them into Parquet format using DuckDB
    3. Stores them locally in a structured folder
    """

    # Create directory: data/<taxi_type> if it doesn't exist
    # Example: data/yellow or data/green
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    # Loop through years 2019 and 2020
    for year in [2019]:
        # Loop through all 12 months
        for month in range(1, 13):

            # Define target Parquet filename
            parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            # If Parquet file already exists, skip processing
            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            # Define compressed CSV filename
            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            # Download the CSV.gz file as a streamed request
            response = requests.get(f"{BASE_URL}/{taxi_type}/{csv_gz_filename}", stream=True)
            response.raise_for_status()  # Raise error if download fails

            # Write downloaded file in chunks to avoid memory overload
            with open(csv_gz_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"Converting {csv_gz_filename} to Parquet...")

            # Open a DuckDB in-memory connection
            con = duckdb.connect()

            # Use DuckDB to:
            # 1. Read CSV automatically (detect schema)
            # 2. Export it as a Parquet file
            con.execute(f"""
                COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}'))
                TO '{parquet_filepath}' (FORMAT PARQUET)
            """)

            # Close DuckDB connection
            con.close()

            # Remove the CSV.gz file to save disk space
            csv_gz_filepath.unlink()

            print(f"Completed {parquet_filename}")


def update_gitignore():
    """
    This function ensures that the 'data/' directory
    is excluded from Git version control.
    """

    gitignore_path = Path(".gitignore")

    # Read existing .gitignore content (if file exists)
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    # If 'data/' is not already excluded, append it
    if 'data/' not in content:
        with open(gitignore_path, 'a') as f:
            f.write('\n# Data directory\ndata/\n' if content else '# Data directory\ndata/\n')


if __name__ == "__main__":
    """
    Main execution block:
    - Updates .gitignore
    - Downloads and converts data
    - Loads Parquet data into DuckDB
    """

    # Ensure data/ folder is ignored by Git
    update_gitignore()

    # Download and convert files for both yellow and green taxis
    for taxi_type in ["fhv"]:
        download_and_convert_files(taxi_type)

    # Create or connect to local DuckDB database file
    con = duckdb.connect("taxi_rides_ny.duckdb")

    # Create production schema if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")

    # For each taxi type, create a table in DuckDB
    # by reading all corresponding Parquet files
    for taxi_type in ["fhv"]:
        con.execute(f"""
            CREATE OR REPLACE TABLE prod.{taxi_type}_tripdata AS
            SELECT * FROM read_parquet('data/{taxi_type}/*.parquet', union_by_name=true)
        """)

    # Close DuckDB connection
    con.close()
