import random, uuid
import pandas as pd
from datetime import timedelta
from faker import Faker
from config.settings import NUM_USERS, NUM_SESSIONS
from src.utils.io import upload_df
from src.utils.logger import get_logger

logger = get_logger(__name__)

def generate_sessions(n: int = NUM_SESSIONS) -> pd.DataFrame:
    fake = Faker()
    logger.info(f"Generating {n} sessions...")
    records = []
    for _ in range(n):
        start = fake.date_time_between(start_date="-30d", end_date="now")
        end   = start + timedelta(minutes=random.randint(1, 120))
        records.append({
            "session_id":    str(uuid.uuid4()),
            "user_id":       random.randint(1, NUM_USERS),
            "session_start": start,
            "session_end":   end,
            "device":        random.choice(["mobile", "web"]),
        })
    df = pd.DataFrame(records)
    logger.info(f"Sessions ready: {len(df):,} rows")
    return df

def run() -> pd.DataFrame:
    df = generate_sessions()
    upload_df(df, layer="raw", name="sessions", fmt="csv")
    return df

if __name__ == "__main__":
    run()
