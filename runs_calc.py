import polars as pl
import adbc_driver_sqlite.dbapi

conn = adbc_driver_sqlite.dbapi.connect("t20matchupviewer.db")



bbb_schema = {
    "match_id": pl.Int64,
    "season": pl.Utf8,
    "start_date": pl.Date,
    "venue": pl.Utf8,
    "innings": pl.UInt8,
    "ball": pl.Utf8,
    "batting_team": pl.Utf8,
    "bowling_team": pl.Utf8,
    "striker": pl.Utf8,
    "non_striker": pl.Utf8,
    "bowler": pl.Utf8,
    "runs_off_bat": pl.UInt8,
    "extras": pl.UInt8,
    "wides": pl.UInt8,
    "noballs": pl.UInt8,
    "byes": pl.UInt8,
    "legbyes": pl.UInt8,
    "penalty": pl.UInt8,
    "wicket_type": pl.Utf8,
    "player_dismissed": pl.Utf8,
    "other_wicket_type": pl.Utf8,
    "other_player_dismissed": pl.Utf8,
    "over" : pl.Utf8
}

query = "SELECT * FROM ball_by_ball order by other_wicket_type DESC"


df = pl.read_database(query,conn,infer_schema_length=100000,schema_overrides=bbb_schema)

print(df.columns)



conn.close()
print(df.head())
