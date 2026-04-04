from src.data_generation.generate_users    import generate_users
from src.data_generation.generate_sessions import generate_sessions
from src.data_generation.generate_events   import generate_events
from src.data_generation.generate_trades   import generate_trades

def test_users_schema():
    df = generate_users(n=10)
    assert len(df) == 10
    assert set(df.columns) == {"user_id","signup_date","country","account_type","risk_profile"}
    assert df["user_id"].is_unique

def test_sessions_schema():
    df = generate_sessions(n=10)
    assert len(df) == 10
    assert (df["session_end"] > df["session_start"]).all()

def test_events_linked_to_sessions():
    sessions = generate_sessions(n=20)
    events   = generate_events(sessions, n=50)
    assert len(events) == 50
    assert events["session_id"].isin(sessions["session_id"]).all()

def test_trades_only_from_place_order():
    sessions = generate_sessions(n=20)
    events   = generate_events(sessions, n=200)
    trades   = generate_trades(events)
    expected = len(events[events["event_type"] == "place_order"])
    assert len(trades) == expected
