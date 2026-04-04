import random, uuid
import pandas as pd
from datetime import timedelta
from config.settings import NUM_EVENTS, EVENT_TYPES
from src.utils.io import upload_df
from src.utils.logger import get_logger

logger = get_logger(__name__)

def generate_events(sessions_df: pd.DataFrame, n: int = NUM_EVENTS) -> pd.DataFrame:
    logger.info(f"Generating {n} events...")
    records = []
    for _ in range(n):
        session  = sessions_df.sample(1).iloc[0]
        duration = max(int((session["session_end"] - session["session_start"]).seconds / 60), 1)
        evt_time = session["session_start"] + timedelta(minutes=random.randint(0, duration))
        records.append({
            "event_id":   str(uuid.uuid4()),
            "user_id":    session["user_id"],
            "session_id": session["session_id"],
            "event_type": random.choice(EVENT_TYPES),
            "event_time": evt_time,
        })
    df = pd.DataFrame(records)
    logger.info(f"Events ready: {len(df):,} rows")
    return df

def run(sessions_df: pd.DataFrame) -> pd.DataFrame:
    df = generate_events(sessions_df)
    upload_df(df, layer="raw", name="events", fmt="csv")
    return df

if __name__ == "__main__":
    from src.data_generation.generate_sessions import generate_sessions
    run(generate_sessions())
