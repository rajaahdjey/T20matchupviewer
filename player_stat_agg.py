import polars as pl
import adbc_driver_sqlite.dbapi

conn = adbc_driver_sqlite.dbapi.connect("t20matchupviewer.db")

query = "SELECT * FROM ball_by_ball where innings <= 2 order by other_wicket_type DESC"


df = pl.read_database(query,conn,infer_schema_length=100000,schema_overrides=bbb_schema)
