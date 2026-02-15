import os
from dotenv import load_dotenv

load_dotenv()

# Database
DB_HOST     = os.getenv("POSTGRES_HOST", "db")
DB_PORT     = int(os.getenv("POSTGRES_PORT", 5432))
DB_NAME     = os.getenv("POSTGRES_DB", "scd2db")
DB_USER     = os.getenv("POSTGRES_USER", "scd2user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "scd2pass")

# Socrata API (NYC Open Data)
SOCRATA_DATASET_ID = "w7w3-xahh"          # NYC active businesses
SOCRATA_BASE_URL   = "https://data.cityofnewyork.us/resource"
SOCRATA_PAGE_SIZE  = 50_000               # rows per API page
SOCRATA_APP_TOKEN  = os.getenv("SOCRATA_APP_TOKEN", "")  # optional, avoids rate limits

# Paths
RAW_DATA_DIR    = "data/raw"
SAMPLE_DATA_DIR = "data/sample"
