import os
import pandas as pd
import json
import psycopg2
from csv import DictReader
from dotenv import load_dotenv

load_dotenv()


def get_current_season_results():
    with open("last_result.json", "r") as json_file:
        data = json.load(json_file)
        last_row = data["last_row"]

    teams = []

    with open("teams.csv", "r") as csv_file:
        dict_reader = DictReader(csv_file)
        teams = list(dict_reader)

    html = pd.read_html(os.getenv("PL_CURRENT_SEASON_URL"), header=0)
    df = (
        html[0][["Date", "Home", "Score", "Away"]]
        .dropna()
        .reset_index()
        .iloc[last_row + 1 :, :]
    )

    results_to_post = []

    for index, row in df.iterrows():
        last_row += 1
        score = row["Score"].split("–")
        home_score = int(score[0])
        away_score = int(score[1])
        home_team_id, home_team_name = get_team(row["Home"], teams)
        away_team_id, away_team_name = get_team(row["Away"], teams)

        results_to_post.append(
            [
                "2023/2024",
                home_team_id,
                home_team_name,
                home_score,
                away_team_id,
                away_team_name,
                away_score,
                row["Date"],
            ]
        )

    if results_to_post:
        post_results(results_to_post)

    print(last_row)
    with open("last_result.json", "w") as json_file:
        json_file.write(json.dumps({"last_row": last_row}))

    with open("last_result.json", "r") as json_file:
        data = json.load(json_file)
        last_row = data["last_row"]
        print(last_row)


def post_results(results):
    try:
        connection = psycopg2.connect(os.getenv("SQLALCHEMY_DATABASE_URI"))
        cursor = connection.cursor()

        for result in results:
            result = tuple(result)
            insert_query = """INSERT INTO match (season, home_team_id, home_team_name, home_score, away_team_id, away_team_name, away_score, date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
            cursor.execute(insert_query, result)

            connection.commit()
            print("Record inserted successfully into match table")

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into table", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


def get_team(team_name, teams):
    for team in teams:
        if team_name == team["fb_ref"]:
            return int(team["team_id"]), team["database"]


def main():
    get_current_season_results()


if __name__ == "__main__":
    main()
