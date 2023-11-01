import os
import pandas as pd
import psycopg2
from csv import DictReader
from dotenv import load_dotenv

load_dotenv()


def connect_to_db():
    connection = psycopg2.connect(os.getenv("SQLALCHEMY_DATABASE_URI"))
    cursor = connection.cursor()
    return connection, cursor


def disconnect_from_db(connection, cursor):
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


def get_current_season_results():
    conn, cur = connect_to_db()
    get_last_row = """SELECT row FROM last_row LIMIT 1"""
    cur.execute(get_last_row)
    last_row = cur.fetchall()[0][0]

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
        post_results(results_to_post, conn, cur)

    update_row(last_row, conn, cur)

    disconnect_from_db(conn, cur)


def post_results(results, conn, cur):
    try:
        for result in results:
            result = tuple(result)
            insert_query = """INSERT INTO match (season, home_team_id, home_team_name, home_score, away_team_id, away_team_name, away_score, date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
            cur.execute(insert_query, result)
            conn.commit()
            print("Record inserted successfully into match table")

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into table", error)


def update_row(last_row, conn, cur):
    try:
        update_query = """UPDATE last_row SET row = %s"""
        cur.execute(update_query, [last_row])
        conn.commit()
        print("Last row updated successfully")

    except (Exception, psycopg2.Error) as error:
        print("Failed to update last_row", error)


def get_team(team_name, teams):
    for team in teams:
        if team_name == team["fb_ref"]:
            return int(team["team_id"]), team["database"]


def main():
    get_current_season_results()


if __name__ == "__main__":
    main()
