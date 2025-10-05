import requests
import json
from google.cloud import bigquery
from datetime import datetime

def extract_and_load_to_bq():
    client = bigquery.Client()
    dataset_id = f"{client.project}.country_pipeline"
    table_id = f"{dataset_id}.countries_raw"

    # Ensure dataset exists
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "EU"
    client.create_dataset(dataset, exists_ok=True)

    # Fetch data from REST Countries API
    url = "https://restcountries.com/v3.1/all?fields=name,capital,currencies"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Prepare rows with safe null handling
    rows = []
    for c in data:
        name = c.get("name", {})
        currencies = c.get("currencies", {})
        capital = c.get("capital", [None])

        rows.append({
            "common_name": name.get("common", None),
            "official_name": name.get("official", None),
            "capital_city": capital[0] if capital else None,
            "currency": ", ".join(currencies.keys()) if currencies else None,
            "raw_json": json.dumps(c, ensure_ascii=False),
            "created_at": datetime.utcnow().isoformat()
        })

    # Define BigQuery schema explicitly for clarity
    schema = [
        bigquery.SchemaField("common_name", "STRING"),
        bigquery.SchemaField("official_name", "STRING"),
        bigquery.SchemaField("capital_city", "STRING"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("raw_json", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP")
    ]

    # Load job config
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    # Insert data into BigQuery
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()

    print(f"Successfully loaded {len(rows)} rows into {table_id}.")
