import random
import pandas as pd
from faker import Faker
from config.settings import NUM_USERS
from src.utils.io import upload_df
from src.utils.logger import get_logger

logger = get_logger(__name__)

def generate_users(n: int = NUM_USERS) -> pd.DataFrame:
    fake = Faker()
    logger.info(f"Generating {n} users...")
    records = [
        {
            "user_id":      i + 1,
            "signup_date":  fake.date_between(start_date="-1y", end_date="today"),
            "country":      random.choice(["India", "USA", "UK"]),
            "account_type": random.choice(["retail", "premium"]),
            "risk_profile": random.choice(["low", "medium", "high"]),
        }
        for i in range(n)
    ]
    df = pd.DataFrame(records)
    logger.info(f"Users ready: {len(df):,} rows")
    return df

def run() -> pd.DataFrame:
    df = generate_users()
    upload_df(df, layer="raw", name="users", fmt="csv")
    return df

if __name__ == "__main__":
    run()
