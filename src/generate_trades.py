# src/generate_trades.py

import pandas as pd
import random
import uuid
from src.gcs_utils import upload_df

STOCKS = ["AAPL","MSFT","GOOGL","AMZN","TSLA","NVDA","META",
          "NFLX","INTC","AMD","ADBE","CSCO","PEP","AVGO",
          "COST","TMUS","QCOM","TXN","AMAT","INTU","PYPL",
          "SBUX","ISRG","BKNG","GILD","ADP","MDLZ","VRTX","LRCX","MU"]

def generate_trades(events_df: pd.DataFrame) -> pd.DataFrame:
    order_events = events_df[events_df["event_type"] == "place_order"]
    trades = []
    for _, event in order_events.iterrows():
        trades.append({
            "trade_id":   str(uuid.uuid4()),
            "user_id":    event["user_id"],
            "stock":      random.choice(STOCKS),
            "trade_type": random.choice(["buy", "sell"]),
            "quantity":   random.randint(1, 50),
            "price":      round(random.uniform(50, 3000), 2),
            "trade_time": event["event_time"],
        })
    df = pd.DataFrame(trades)
    print(f"  Generated {len(df)} trades")
    return df

def run(events_df: pd.DataFrame):
    df = generate_trades(events_df)
    upload_df(df, "raw", "trades", fmt="csv")
    return df

if __name__ == "__main__":
    from src.generate_sessions import generate_sessions
    from src.generate_events import generate_events
    sessions = generate_sessions()
    events   = generate_events(sessions)
    run(events)
