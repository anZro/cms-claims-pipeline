import duckdb
import os

DB_PATH = "cms_claims_pipeline/dev.duckdb"
RAW_DIR = "data/raw"

PARTD_FILE = os.path.join(RAW_DIR, "DSD_PTD_RY25_P04_V10_DY23_BGM.csv")
PROVIDER_FILE = os.path.join(RAW_DIR, "MUP_PHY_R25_P05_V20_D23_Prov_Svc.csv")

con = duckdb.connect(DB_PATH)

con.execute("CREATE SCHEMA IF NOT EXISTS raw")

con.execute(f"""
    CREATE OR REPLACE TABLE raw.cms_partd_raw AS
    SELECT * FROM read_csv_auto('{PARTD_FILE}', header=true)
""")

con.execute(f"""
    CREATE OR REPLACE TABLE raw.cms_provider_raw AS
    SELECT * FROM read_csv_auto('{PROVIDER_FILE}', header=true)
""")

partd_count = con.execute("SELECT COUNT(*) FROM raw.cms_partd_raw").fetchone()[0]
provider_count = con.execute("SELECT COUNT(*) FROM raw.cms_provider_raw").fetchone()[0]

print(f"✅ cms_partd_raw loaded: {partd_count:,} rows")
print(f"✅ cms_provider_raw loaded: {provider_count:,} rows")

con.close()
