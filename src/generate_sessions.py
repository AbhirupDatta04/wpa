# src/generate_sessions.py

import pandas as pd
import random
import uuid
from datetime import timedelta
from faker import Faker
from src.config import NUM_USERS, NUM_SESSIONS
from src.gcs_utils import upload_df

def generate_sessions() -> pd.DataFrame:
    fake = Faker()
    sessions = []
    for _ in range(NUM_SESSIONS):
        start = fake.date_time_between(start_date="-30d", end_date="now")
        end   = start + timedelta(minutes=random.randint(1, 120))
        sessions.append({
            "session_id":    str(uuid.uuid4()),
            "user_id":       random.randint(1, NUM_USERS),
            "session_start": start,
            "session_end":   end,
            "device":        random.choice(["mobile", "web"]),
        })
    df = pd.DataFrame(sessions)
    print(f"  Generated {len(df)} sessions")
    return df

def run():
    df = generate_sessions()
    upload_df(df, "raw", "sessions", fmt="csv")
    return df

if __name__ == "__main__":
    run()
