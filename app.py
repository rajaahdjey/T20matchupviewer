from flask import Flask, render_template, request
import requests
import json
import plotly.express as px
import adbc_driver_sqlite.dbapi
import plotly


app = Flask(__name__)

conn = adbc_driver_sqlite.dbapi.connect("t20matchupviewer.db")
bat_unique = f"SELECT distinct striker FROM ball_by_ball"
bowl_unique = f"SELECT distinct bowler FROM ball_by_ball"
# players = ["MS Dhoni","R Parag","KA Pollard","KL Rahul"]
cursor = conn.cursor()
cursor.execute(bat_unique)
players = set([row[0] for row in cursor.fetchall()])
print("Players List:", players)

cursor.execute(bowl_unique)
players = players.union(set([row[0] for row in cursor.fetchall()]))
cursor.close()
conn.close()
print("Players List:", players)


@app.route("/", methods=["GET", "POST"])
@app.route("/home", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        selected_player = request.form.get("player")

        response = requests.get(
            f"http://127.0.0.1:8000/true_strike_rate_trend",
            params={"player": selected_player},
            timeout=100,
        )
        filtered_data = response.json()

        if filtered_data:
            df = {
                "over": filtered_data["over"],
                "true_strike_rate": filtered_data["true_strike_rate"],
                "ball": filtered_data["ball"],
            }

            fig = px.line(
                df,
                x="over",
                y="true_strike_rate",
                title=f"True Strike Rate Trend for {selected_player} (min 30 balls faced in over slot)",
                template="plotly_dark",
            )
            plotJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

            return render_template(
                "index.html",
                players=players,
                selected_player=selected_player,
                plotJSON=plotJSON,
            )

    return render_template("index.html", players=players, plotJSON=None)


@app.route("/about")
def about():
    return render_template("about.html", players=players, plotJSON=None)


if __name__ == "__main__":
    app.run(port=5000)
