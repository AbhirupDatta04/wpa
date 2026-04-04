# src/generate_users.py

import pandas as pd
import random
from faker import Faker
from src.config import NUM_USERS
from src.gcs_utils import upload_df

def generate_users() -> pd.DataFrame:
    fake = Faker()
    users = []
    for i in range(NUM_USERS):
        users.append({
            "user_id":      i + 1,
            "signup_date":  fake.date_between(start_date="-1y", end_date="today"),
            "country":      random.choice(["India", "USA", "UK"]),
            "account_type": random.choice(["retail", "premium"]),
            "risk_profile": random.choice(["low", "medium", "high"]),
        })
    df = pd.DataFrame(users)
    print(f"  Generated {len(df)} users")
    return df

def run():
    df = generate_users()
    upload_df(df, "raw", "users", fmt="csv")
    return df

if __name__ == "__main__":
    run()
