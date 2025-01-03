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
    "over": pl.UInt8,
}

# innings of above 2 indicates super over.
query = "SELECT * FROM ball_by_ball where innings <= 2 order by other_wicket_type DESC"


df = pl.read_database(
    query, conn, infer_schema_length=100000, schema_overrides=bbb_schema
)

print(df.columns)


def expected_runs_calc(df : pl.DataFrame):
    #only wide is not counted in balls faced and deliveries bowled.
    df = df.with_columns(batsman_ball = pl.when((pl.col("wides").is_not_null())).then(None).otherwise(pl.col("ball")))
    df = df.with_columns(ball = pl.when((pl.col("wides").is_not_null())|(pl.col("noballs").is_not_null())).then(None).otherwise(pl.col("ball")))
    expected_runs = df.group_by(["league", "year", "over"]).agg(
        [
            pl.count("ball"),
            pl.count("batsman_ball"),
            pl.sum("runs_off_bat"),
            pl.sum("extras"),
            pl.count("wicket_type"),
        ]
    )

    expected_runs = expected_runs.with_columns(
        expected_bowling_economy=(
            pl.sum_horizontal("runs_off_bat", "extras") * 6.0 / pl.col("ball")
        )
    )
    expected_runs = expected_runs.with_columns(
        expected_batting_strike_rate=(
            pl.sum_horizontal("runs_off_bat") * 100.0 / pl.col("batsman_ball")
        )
    )
    expected_runs = expected_runs.with_columns(
        expected_bowling_average=(
            pl.sum_horizontal("runs_off_bat", "extras") / pl.col("wicket_type")
        )
    )
    expected_runs = expected_runs.with_columns(
        expected_batting_average=(
            pl.sum_horizontal("runs_off_bat") / pl.col("wicket_type")
        )
    )
    expected_runs = expected_runs.with_columns(
        expected_bowling_strike_rate=(pl.col("ball") / pl.col("wicket_type"))
    )
    expected_runs = expected_runs.sort(["league", "year", "over"])

    return expected_runs


#################################################################
# only for plotting and seeing trends now , not part of final app#
#################################################################
# import seaborn as sns
# import matplotlib.pyplot as plt
# g = sns.FacetGrid(data=expected_runs.filter(pl.col("league")=="IPL_T20").to_pandas(),col="year",hue="year")
# g.map(sns.lineplot,"over","expected_bowling_economy")
# g.add_legend()
# plt.show()


conn.close()

expected_runs_overall = expected_runs_calc(df)
expected_runs_overall.write_database(
    connection="sqlite:///t20matchupviewer.db",
    table_name="expected_runs",
    engine="adbc",
    if_table_exists="replace",
)

# print(df.head())
