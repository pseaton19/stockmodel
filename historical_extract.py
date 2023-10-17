import os
import requests
import pandas as pd
from google.cloud import bigquery
from pathlib import Path
from datetime import datetime, timedelta
from decouple import config

# Set the path to your service account key file
service_account_key_path = Path(config("GOOGLE_APPLICATION_CREDENTIALS"))

def fetch_and_upload_stock_data():
    """

    Author: Tim Burris
    Creation Date: 10/14/2023
    Modified Date: 10/19/2023
    
    This function extracts historical stock market data for a list of stock symbols
    for the past 2 years using the Polygon.io API, ending on a manually set day of 10-17-2023.
    It then transforms and prepares the data, adding additional columns such as 'original_upload'
    and 'last_modified'. Finally, it saves the data to CSV files and uploads
    it to the "MAANGM" BigQuery table.
    """
    # Polygon.io API endpoint
    api_url = "https://api.polygon.io/v2/aggs/ticker/{stock}/range/{multiplier}/{timespan}/{from_date}/{to_date}"

    # Replace with your Polygon.io API key
    api_key = config('POLYGON_API_KEY')

    # Replace with your Google Cloud project ID
    project_id = config('BIGQUERY_PROJECT_ID')

    # Define the stock symbols
    stock_symbols = ["META", "AMZN", "NFLX", "GOOGL"]

    # Define the date range for the last 2 years
    today = datetime.strptime("2023-10-17", '%Y-%m-%d')
    two_years_ago = today - timedelta(days=730)  # Approximately 365 days per year
    date_from = two_years_ago.strftime('%Y-%m-%d')
    date_to = today.strftime('%Y-%m-%d')

    # Define the output directory
    output_dir = "output"

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Initialize a list to store the dataframes for each stock
    dataframes = []

    # Define the mapping for column names
    column_mapping = {
        'o': 'open',
        'h': 'high',
        'l': 'low',
        'c': 'close',
        'v': 'volume',
        'vw': 'vwap',
        't': 'timestamp',
    }

    # Define the schema for your BigQuery table
    schema = [
        bigquery.SchemaField('original_upload', 'TIMESTAMP'),
        bigquery.SchemaField('last_modified', 'TIMESTAMP'),  
        bigquery.SchemaField('timestamp', 'TIMESTAMP'),
        bigquery.SchemaField('symbol', 'STRING'),
        bigquery.SchemaField('open', 'FLOAT'),
        bigquery.SchemaField('high', 'FLOAT'),
        bigquery.SchemaField('low', 'FLOAT'),
        bigquery.SchemaField('close', 'FLOAT'),
        bigquery.SchemaField('volume', 'INTEGER'),
        bigquery.SchemaField('vwap', 'FLOAT'),
    ]

    # Loop through each stock symbol and retrieve data
    for stock_symbol in stock_symbols:
        # Construct the API URL
        url = api_url.format(
            stock=stock_symbol,
            multiplier=1,  # Adjust as needed
            timespan="day",  # Adjust as needed
            from_date=date_from,
            to_date=date_to
        ) + f"?apiKey={api_key}"

        # Send a GET request to the API
        response = requests.get(url)

        if response.status_code == 200:
            # Parse the JSON response into a Pandas DataFrame
            data = response.json()
            df = pd.DataFrame(data.get("results", []))

            # Rename the columns based on the mapping
            df = df.rename(columns=column_mapping)

            for field in schema:
                if field.name not in df.columns:
                    df[field.name] = None

            # Add a 'symbol' column
            df['symbol'] = stock_symbol

            # Add 'original_upload' column with the current timestamp
            df['original_upload'] = datetime.now()

            # Determine the set of columns dynamically
            columns = [field.name for field in schema]
            missing_columns = set(columns) - set(df.columns)

            # Add missing columns to the dataframe with NaN values
            for column in missing_columns:
                df[column] = None

            # Select and reorder columns
            selected_columns = ['original_upload', 'last_modified', 'timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'vwap']
            df = df[selected_columns]

            # Convert 'timestamp' column to a valid timestamp format
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', origin='unix')

            # Convert 'volume' column to INT64
            df['volume'] = df['volume'].astype('int64')

            dataframes.append(df)

            # Save the data as a CSV file
            csv_filename = os.path.join(output_dir, f"{stock_symbol}_data.csv")
            df.to_csv(csv_filename, index=False)
            print(f"--> Data for {stock_symbol} saved as {csv_filename}")
        else:
            print(f"WARNING: Failed to retrieve data for {stock_symbol}. Status code: {response.status_code}")

    # Concatenate all dataframes into a single dataframe
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Ensure that the data types match the schema
    for field in schema:
        if field.name in combined_df.columns:
            if field.field_type == 'TIMESTAMP':
                combined_df[field.name] = pd.to_datetime(combined_df[field.name])
            elif field.field_type == 'FLOAT':
                combined_df[field.name] = combined_df[field.name].astype(float)
            elif field.field_type == 'INTEGER':
                combined_df[field.name] = combined_df[field.name].astype(int)

    # Upload the combined data to BigQuery
    client = bigquery.Client.from_service_account_json(service_account_key_path)

    dataset_id = config("POLYGON_DATASET_ID")
    table_id = config("POLYGON_TABLE")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip the CSV header
        schema=schema,  # Specify the schema for the table
    )

    # Save the combined data as a CSV file
    combined_csv_filename = os.path.join(output_dir, "combined_stock_data.csv")
    combined_df.to_csv(combined_csv_filename, index=False)
    print(f"Combined data saved as {combined_csv_filename}")

    # Upload the combined data to BigQuery
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    with open(combined_csv_filename, "rb") as source_file:
        job = client.load_table_from_file(
            source_file, table_ref, job_config=job_config
        )

    job.result()  # Wait for the job to complete
    print(f"Combined data uploaded to BigQuery dataset '{dataset_id}' table '{table_id}'")

if __name__ == "__main__":
    fetch_and_upload_stock_data()
