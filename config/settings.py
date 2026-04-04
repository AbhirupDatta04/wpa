import os

GCP_PROJECT  = os.getenv("GCP_PROJECT", "your-gcp-project-id")
BUCKET_NAME  = os.getenv("GCS_BUCKET",  "wealth-analytics-data-lake")

RAW_PATH    = f"gs://{BUCKET_NAME}/raw"
BRONZE_PATH = f"gs://{BUCKET_NAME}/bronze"
SILVER_PATH = f"gs://{BUCKET_NAME}/silver"
GOLD_PATH   = f"gs://{BUCKET_NAME}/gold"

NUM_USERS    = 1000
NUM_SESSIONS = 3000
NUM_EVENTS   = 10_000

DATASETS = ["users", "sessions", "events", "trades"]

STOCKS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META",
    "NFLX", "INTC", "AMD",   "ADBE", "CSCO", "PEP",  "AVGO",
    "COST", "TMUS", "QCOM",  "TXN",  "AMAT", "INTU", "PYPL",
    "SBUX", "ISRG", "BKNG",  "GILD", "ADP",  "MDLZ", "VRTX",
    "LRCX", "MU",
]

EVENT_TYPES = [
    "login", "view_stock", "search_stock",
    "add_watchlist", "place_order", "logout",
]
