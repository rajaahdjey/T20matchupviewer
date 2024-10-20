import csv
from collections import defaultdict
from pathlib import Path
import json
import os
import sqlite3
import polars as pl

#hundred over rules are irritating, we don't want hundred for now.


THIS_DIR = Path(__file__).parent

data_source = THIS_DIR / "./data_sources.json"

with open(data_source, "r", encoding="utf8") as file:
    data_source_json = json.load(file)
    DOWNLOAD_LIST = data_source_json["match_data"]
    PEOPLE_LIST = data_source_json["aux_data"]


# for key,_ in DOWNLOAD_LIST.items():

test_csv = r"D:\DS_Workspace\Github2.0\T20matchupviewer\extracted_data\BBL_T20\1386134_info.csv"

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
    "league" : pl.Utf8,
    "year" : pl.UInt16,
    "over" : pl.UInt8
}
ball_by_ball = pl.DataFrame(schema=bbb_schema)

# for first iter, skip INT T20 as they do not have the all_matches.csv. Parse them match by match later.
for league, _ in DOWNLOAD_LIST.items():
    if league != "INT_T20":
        all_matches_path = THIS_DIR / f"./extracted_data/{league}/all_matches.csv"
        all_matches_df = pl.read_csv(all_matches_path, dtypes=bbb_schema)
        all_matches_df = all_matches_df.with_columns(league=pl.lit(league))
        all_matches_df = all_matches_df.with_columns(year=pl.col("start_date").dt.year().cast(pl.UInt16))
        all_matches_df = all_matches_df.with_columns(over=pl.col("ball").cast(pl.Float32).floor().cast(pl.UInt8)+1)
        ball_by_ball = pl.concat([ball_by_ball, all_matches_df])


conn = sqlite3.connect("t20matchupviewer.db")

ball_by_ball.write_database(
    connection="sqlite:///t20matchupviewer.db",
    table_name="ball_by_ball",
    engine="adbc",
    if_table_exists= "replace",
)

conn.close()

print(ball_by_ball.shape)


def parse_custom_csv(file_path):
    data = defaultdict(list)
    league = None
    match_id = os.path.splitext(os.path.basename(file_path))[0]
    version_info = None
    with open(file_path, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if row[0] == "version":
                version_info = row[1]
            elif row[0] == "info":
                key = row[1]
                if len(row) == 3:
                    data[key].append(row[2])
                elif len(row) > 3:
                    print("Asda")
