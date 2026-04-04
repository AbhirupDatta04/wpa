from src.utils.logger import get_logger
from src.data_generation.generate_users    import run as run_users
from src.data_generation.generate_sessions import run as run_sessions
from src.data_generation.generate_events   import run as run_events
from src.data_generation.generate_trades   import run as run_trades

logger = get_logger(__name__)

def run_generation_pipeline() -> dict:
    logger.info("=" * 50)
    logger.info("DATA GENERATION PIPELINE — START")
    logger.info("=" * 50)

    users_df    = run_users()
    sessions_df = run_sessions()
    events_df   = run_events(sessions_df)
    trades_df   = run_trades(events_df)

    logger.info("=" * 50)
    logger.info("DATA GENERATION PIPELINE — DONE")
    logger.info(f"  users    : {len(users_df):,}")
    logger.info(f"  sessions : {len(sessions_df):,}")
    logger.info(f"  events   : {len(events_df):,}")
    logger.info(f"  trades   : {len(trades_df):,}")
    logger.info("=" * 50)

    return {
        "users":    users_df,
        "sessions": sessions_df,
        "events":   events_df,
        "trades":   trades_df,
    }

if __name__ == "__main__":
    run_generation_pipeline()
