# End-to-End Azure Data Engineering Project
## Medallion (bronze → silver → gold) on ADF + Databricks (Autoloader, DLT SCD2) with CI/CD

A complete, Azure lakehouse that demonstrates real-world ingestion, streaming/batch processing, dimensional modeling, and deployment automation.

## Description

This project is a full Azure Data Engineering build that ingests from a cloud-hosted Azure SQL Database into ADLS Gen2 using Azure Data Factory (ADF) with incremental loading and backfilling (not a full refresh). Data is refined in Azure Databricks with Spark Structured Streaming + Autoloader, governed by Unity Catalog, and modeled into a star schema with Slowly Changing Dimensions (SCD Type 2). The Gold layer is curated via Delta Live Tables (DLT), and deployments follow CI/CD best practices using Databricks Asset Bundles and GitHub. Logic Apps provide email alerts on ADF failures. The repo also covers the full resource setup (RG, Storage with bronze/silver/gold, ADF, SQL DB, Databricks workspace).

<img src="screenshots/azure-resource-group.png" alt="Azure Resource Group with Storage, ADF, Databricks, SQL, Access Connector" />

## Azure Data Factory And SQL

### Resource setup (Azure)

 Create a Resource Group, Storage account (containers: bronze/, silver/, gold/), Azure Data Factory, Azure SQL (source), Azure Databricks workspace, and Access Connector for Databricks → ADLS.
 
 <img src="screenshots/azure-resource-group.png" alt="Azure Resource Group with Storage, ADF, Databricks, SQL, Access Connector" />

### Ingestion — ADF (Bronze)

Goal: load data from Azure SQL to ADLS incrementally, support backfill, and keep bronze clean (delete empty files).

* Metadata-driven ForEach: iterate tables with {schema, table, cdc_col, from_date}.
<img src="screenshots/adf-foreach.png" alt="ADF ForEach pipeline with lookup → copy → if condition" />

* Copy (SQL → Data Lake): dynamic container/folder/file, writes parquet like bronze/(table)/(table)_(current_ts).parquet.
<img src="screenshots/adf-copy-sink.png" alt="ADF Copy sink using dynamic container/folder/file for parquet" />

* Watermarking:

    * If True (rowsCopied > 0): SELECT MAX(cdc_col) then update bronze/(table)/cdc.json.
    <img src="screenshots/adf-if-true-maxcdc.png" alt="ADF Script SELECT MAX(cdc_col) and update_last_cdc copy activity" />

    * If False (rowsCopied == 0): delete the empty file (keeps bronze tidy).
    <img src="screenshots/adf-if-false-delete.png" alt="ADF Delete activity removes empty file when no new rows" />

### Loop input example (docs/loop_input.json):

```
[
  {"schema":"dbo","table":"DimUser","cdc_col":"updated_at","from_date":""},
  {"schema":"dbo","table":"DimTrack","cdc_col":"updated_at","from_date":""},
  {"schema":"dbo","table":"DimDate","cdc_col":"date","from_date":""},
  {"schema":"dbo","table":"DimArtist","cdc_col":"updated_at","from_date":""},
  {"schema":"dbo","table":"FactStream","cdc_col":"stream_timestamp","from_date":""}
]
```

### Incremental source query (Copy → source):

```
SELECT * FROM @{item().schema}.@{item().table}
WHERE @{item().cdc_col} > '@{if(empty(item().from_date),activity('last_cdc').output.value[0].cdc,item().from_date)}'
```

### MAX(cdc) (If-True → Script):

```
SELECT MAX(@{item().cdc_col}) AS cdc
FROM @{item().schema}.@{item().table}
```

## DataBricks

Stream raw files from /bronze using Autoloader, apply reusable transforms, and persist clean Delta tables in /silver governed by Unity Catalog.

### Processing — Databricks (Silver)

* Autoloader + Structured Streaming reads /bronze/(table)/, uses checkpoints, and writes Delta to /silver. 
* Create catalog/schema and external locations for bronze/, silver/, gold/ pointing at ADLS via the Access Connector credential.
* Reusable Python modules (utilities/transformations: drop duplicates, column transforms) for DRY, modular code.
* Clusters: serverless for dev or all-purpose as needed.

* Example Autoloader code snippt
```
from pyspark.sql.functions import *

source_dir = "abfss://bronze@<storage>.dfs.core.windows.net/dimuser/"
checkpoint = "abfss://silver@<storage>.dfs.core.windows.net/_checkpoints/dimuser/"
schema_loc = "abfss://silver@<storage>.dfs.core.windows.net/_schemas/dimuser/"
silver_tbl = "spotify.silver.dimuser"

raw = (spark.readStream.format("cloudFiles")
       .option("cloudFiles.format", "parquet")
       .option("cloudFiles.schemaLocation", schema_loc)
       .load(source_dir))

# Example inline transforms (you also use your utility functions below)
clean = (raw
         .withColumn("user_name", upper(trim(col("user_name"))))
         .dropDuplicates(["user_id"]))

(clean.writeStream
 .format("delta")
 .option("checkpointLocation", checkpoint)
 .trigger(once=True)                  # batch-like streaming
 .toTable(silver_tbl))                # creates UC table spotify.silver.dimuser
```

### Reusable utilities for Silver
* utilities module so code can be reused across tables/projects.
* Folder layout
```
 spotify_dab/
   ├─ silver/
   │  └─ silver_Dimensions.ipynb
   └─ utils/
      ├─ transformations.py      # drop_column
```

### Modeling — DLT (Gold)

* Streaming staging → streaming tables → auto-CDC.
    Each module reads Silver with readStream, defines streaming target tables, and wires an auto-CDC flow (keys + sequence) to keep Gold continuously aligned with upstream.
* Dim = SCD2; Fact = SCD1/merge.
    Dimensions track full history via stored_as_scd_type=2 and sequence_by (e.g., updated_at), while the fact table uses SCD1-style upsert for clean, last-write-wins behavior.
        <img src="screenshots/adf-if-false-delete.png" alt="ADF Delete activity removes empty file when no new rows" />
* Data quality with expectations.
    Rules like user_id IS NOT NULL (drop-on-violation) prevent bad records from entering Gold while surfacing metrics in the DLT UI for observability.
* Operational robustness by design.
    DLT provides checkpointing, retries, lineage DAG, and quality metrics out-of-the-box, making flows idempotent, resilient to restarts, and easy to debug.
         <img src="screenshots/adf-if-false-delete.png" alt="ADF Delete activity removes empty file when no new rows" />


###
Final Words

This project isn’t a toy pipeline—it’s a realistic Azure lakehouse that showcases the exact patterns teams ship in production: incremental ingestion, streaming transforms with Autoloader, SCD2 dimensions via DLT, and governed Unity Catalog with CI/CD. Clone it, run it, and adapt it to your next dataset; the structure, utilities, and metadata-driven approach are built to be reused and extended as your data platform grows.