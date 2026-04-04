import random, uuid
import pandas as pd
from config.settings import STOCKS
from src.utils.io import upload_df
from src.utils.logger import get_logger

logger = get_logger(__name__)

def generate_trades(events_df: pd.DataFrame) -> pd.DataFrame:
    orders = events_df[events_df["event_type"] == "place_order"].copy()
    logger.info(f"Generating trades from {len(orders):,} place_order events...")
    records = [
        {
            "trade_id":   str(uuid.uuid4()),
            "user_id":    row["user_id"],
            "stock":      random.choice(STOCKS),
            "trade_type": random.choice(["buy", "sell"]),
            "quantity":   random.randint(1, 50),
            "price":      round(random.uniform(50, 3000), 2),
            "trade_time": row["event_time"],
        }
        for _, row in orders.iterrows()
    ]
    df = pd.DataFrame(records)
    logger.info(f"Trades ready: {len(df):,} rows")
    return df

def run(events_df: pd.DataFrame) -> pd.DataFrame:
    df = generate_trades(events_df)
    upload_df(df, layer="raw", name="trades", fmt="csv")
    return df

if __name__ == "__main__":
    from src.data_generation.generate_sessions import generate_sessions
    from src.data_generation.generate_events   import generate_events
    run(generate_events(generate_sessions()))
