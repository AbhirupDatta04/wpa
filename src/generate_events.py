# src/generate_events.py

import pandas as pd
import random
import uuid
from datetime import timedelta
from src.config import NUM_EVENTS
from src.gcs_utils import upload_df

EVENT_TYPES = ["login", "view_stock", "search_stock",
               "add_watchlist", "place_order", "logout"]

def generate_events(sessions_df: pd.DataFrame) -> pd.DataFrame:
    events = []
    for _ in range(NUM_EVENTS):
        session  = sessions_df.sample(1).iloc[0]
        duration = int((session["session_end"] - session["session_start"]).seconds / 60)
        evt_time = session["session_start"] + timedelta(minutes=random.randint(0, max(duration, 1)))
        events.append({
            "event_id":   str(uuid.uuid4()),
            "user_id":    session["user_id"],
            "session_id": session["session_id"],
            "event_type": random.choice(EVENT_TYPES),
            "event_time": evt_time,
        })
    df = pd.DataFrame(events)
    print(f"  Generated {len(df)} events")
    return df

def run(sessions_df: pd.DataFrame):
    df = generate_events(sessions_df)
    upload_df(df, "raw", "events", fmt="csv")
    return df

if __name__ == "__main__":
    from src.generate_sessions import generate_sessions
    run(generate_sessions())
