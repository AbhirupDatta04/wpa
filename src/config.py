# src/config.py
# Central config — import this in every module

BUCKET_NAME = "wealth-analytics-data-lake"

PATHS = {
    "raw":    f"gs://{BUCKET_NAME}/raw/",
    "bronze": f"gs://{BUCKET_NAME}/bronze/",
    "silver": f"gs://{BUCKET_NAME}/silver/",
    "gold":   f"gs://{BUCKET_NAME}/gold/",
}

DATASETS   = ["users", "sessions", "events", "trades"]
NUM_USERS  = 1000
NUM_SESSIONS = 3000
NUM_EVENTS = 10000
