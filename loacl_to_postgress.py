import pandas as pd
from sqlalchemy import create_engine


# Postgres connection



engine = create_engine("postgresql://admin:admin123@18.134.163.221:5432/testdb")


# CSV File Paths

player_stats_path = r"C:\Users\Rajeswari Pinnaka\Downloads\PlayerStatistics_1.csv"
players_path = r"C:\Users\Rajeswari Pinnaka\Downloads\Players_1.csv"
games_path = r"C:\Users\Rajeswari Pinnaka\Downloads\Games_1.csv"
team_stats_path = r"C:\Users\Rajeswari Pinnaka\Downloads\TeamStatistics_1.csv"
team_histories_path = r"C:\Users\Rajeswari Pinnaka\Downloads\TeamHistories_1.csv"  # your uploaded file


# Load CSVs into DataFrames

df_player_stats = pd.read_csv(player_stats_path)
df_players = pd.read_csv(players_path)
df_games = pd.read_csv(games_path)
df_team_stats = pd.read_csv(team_stats_path)
df_team_histories = pd.read_csv(team_histories_path)


# Load data into PostgreSQL

df_player_stats.to_sql("player_statistics", engine, if_exists="replace", index=False)
df_players.to_sql("players", engine, if_exists="replace", index=False)
df_games.to_sql("games", engine, if_exists="replace", index=False)
df_team_stats.to_sql("team_statistics", engine, if_exists="replace", index=False)
df_team_histories.to_sql("team_histories", engine, if_exists="replace", index=False)

print(" Successfully loaded all 5 tables into PostgreSQL (raji_db)!")
