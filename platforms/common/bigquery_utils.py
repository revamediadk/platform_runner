import os
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def ensure_staging_table(client, staging_table_id: str):
    print(f"🔎 Checking if staging table `{staging_table_id}` exists and matches expected schema...")
    try:
        client.get_table(staging_table_id)
        print("✅ Staging table already exists.")
    except NotFound:
        print("⚠️ Staging table not found. Creating it...")
        dataset_id, table_name = staging_table_id.split('.')[-2:]
        dataset_ref = client.dataset(dataset_id)

        schema = [
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("cost", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("conversions", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("market_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("market_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("market_sector", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("insert_id", "STRING", mode="REQUIRED"),
        ]

        table = bigquery.Table(staging_table_id, schema=schema)
        client.create_table(table)
        print("✅ Staging table created successfully.")

def insert_to_bigquery(rows: list, table_id: str):
    try:
        client = bigquery.Client.from_service_account_json("secrets/bq_db.json")

        enriched_rows = []
        for row in rows:
            # Standardize date
            if isinstance(row['date'], str):
                date_obj = datetime.strptime(row['date'], "%Y-%m-%d").date()
            else:
                date_obj = row['date']
            row['date'] = date_obj.isoformat()

            # Cast to match BigQuery types
            row['market_id'] = str(row['market_id'])
            row['cost'] = float(row['cost'])
            row['conversions'] = int(row['conversions'] or 0)
            row['market_name'] = str(row['market_name'])
            row['market_sector'] = str(row['market_sector'])

            # Create insert_id
            row['insert_id'] = f"{row['market_id']}_{row['date']}"

            enriched_rows.append(row)

        # Collect unique partition dates for filtering
        unique_dates = sorted(set(row['date'] for row in enriched_rows))
        date_list_str = ", ".join(f"DATE('{d}')" for d in unique_dates)

        # Define and ensure staging table exists
        staging_table_id = table_id + "_staging"
        ensure_staging_table(client, staging_table_id)

        # Upload to staging table with WRITE_TRUNCATE
        print(f"📦 Uploading to staging table: {staging_table_id}")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        load_job = client.load_table_from_json(enriched_rows, staging_table_id, job_config=job_config)
        load_job.result()
        print("✅ Upload to staging complete.")

        # MERGE into main table with partition filter
        print("🔁 Running MERGE into main table...")
        merge_query = f"""
        MERGE `{table_id}` AS main
        USING (
        SELECT
            CAST(date AS DATE) AS date,
            CAST(cost AS FLOAT64) AS cost,
            CAST(conversions AS INT64) AS conversions,
            CAST(market_name AS STRING) AS market_name,
            CAST(market_id AS STRING) AS market_id,
            CAST(market_sector AS STRING) AS market_sector,
            CAST(insert_id AS STRING) AS insert_id
        FROM `{staging_table_id}`
        WHERE date IN ({date_list_str})
        ) AS staging
        ON main.insert_id = staging.insert_id AND main.date = staging.date
        WHEN MATCHED THEN
        UPDATE SET
            main.cost = staging.cost,
            main.conversions = staging.conversions,
            main.market_name = staging.market_name,
            main.market_id = staging.market_id,
            main.market_sector = staging.market_sector,
            main.updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
        INSERT (
            date, cost, conversions, market_name, market_id,
            market_sector, insert_id, created_at, updated_at
        )
        VALUES (
            staging.date,
            staging.cost,
            staging.conversions,
            staging.market_name,
            staging.market_id,
            staging.market_sector,
            staging.insert_id,
            CURRENT_TIMESTAMP(),  -- created_at
            CURRENT_TIMESTAMP()   -- updated_at
        )
        """
        merge_job = client.query(merge_query)
        merge_job.result()
        print("✅ Merge into main table completed.")

    except Exception as e:
        import traceback
        print("❌ Failed to insert into BigQuery.")
        traceback.print_exc()
