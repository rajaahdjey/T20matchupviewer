from fastapi import FastAPI,Query
from fastapi.responses import JSONResponse
import polars as pl
import adbc_driver_sqlite.dbapi
from runs_calc import expected_runs_calc
import pydantic

app = FastAPI()


def player_filter_batsman(selected_player):
    conn = adbc_driver_sqlite.dbapi.connect("t20matchupviewer.db")
    player_id_query = f"SELECT key_cricinfo FROM people where unique_name = '{selected_player}'"
    cursor = conn.cursor()
    cursor.execute(player_id_query)
    selected_id = cursor.fetchone()
    selected_id = selected_id[0]
    cursor.close()
    query = f"SELECT striker,bowler,extras,wides,noballs,ball,runs_off_bat,wicket_type,league,year,over FROM ball_by_ball where innings <=2 AND striker = '{selected_id}' order by other_wicket_type DESC"

    df = pl.read_database(query, conn)
    
    expected_benchmark_strike_rate = pl.read_database(
        "select * FROM expected_runs order by league,year,over", conn
    )[["league", "year", "over", "expected_batting_strike_rate"]]

    conn.close()

    expected_runs_player = expected_runs_calc(df)[
        ["ball", "league", "year", "over", "expected_batting_strike_rate"]
    ]

    expected_runs_player = expected_runs_player.rename(
        {"expected_batting_strike_rate": "player_batting_strike_rate"}
    )
    expected_runs_player = expected_runs_player.join(
        expected_benchmark_strike_rate,
        on=["league", "year", "over"],
        how="left",
        coalesce=True,
    )
    expected_runs_player = expected_runs_player.with_columns(
        true_strike_rate=pl.col("player_batting_strike_rate")
        * 100.0
        / pl.col("expected_batting_strike_rate")
    )
    expected_runs_player = expected_runs_player.group_by("over").agg(
        [
            (pl.col("true_strike_rate") * pl.col("ball")).sum() / pl.col("ball").sum(),
            pl.col("ball").sum(),
        ]
    )

    return expected_runs_player.sort(by="over")


print(player_filter_batsman("MS Dhoni"))

@app.get("/true_strike_rate_trend")
def get_true_strike_rate_trend(player: str = Query("MS Dhoni")):
    return JSONResponse(player_filter_batsman(player).filter(pl.col("ball")>30).to_dict(as_series=False)) #min 30 balls faced in that over slot 
    


#########################################
#Plot below only for checks / blog posts#
#########################################

# import seaborn as sns
# import matplotlib.pyplot as plt
# fig,ax = plt.subplots(figsize=(16,9))
# # sns.lineplot(ax = ax,data=player_filter_batsman("KL Rahul").filter(pl.col("ball")>50),x="over",y="true_strike_rate",label="KL Rahul")
# # sns.lineplot(ax = ax,data=player_filter_batsman("KA Pollard").filter(pl.col("ball")>50),x="over",y="true_strike_rate",label="KA Pollard")
# # sns.lineplot(ax = ax,data=player_filter_batsman("MS Dhoni").filter(pl.col("ball")>50),x="over",y="true_strike_rate",label="MS Dhoni")
# # sns.lineplot(ax = ax,data=player_filter_batsman("JC Buttler").filter(pl.col("ball")>50),x="over",y="true_strike_rate",label="JC Buttler")
# plt.show()

