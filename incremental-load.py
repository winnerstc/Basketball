# coding: utf-8
# kaggle_to_postgres_filtered.py

import os
import pandas as pd
import kagglehub
from sqlalchemy import create_engine

# ----------------------------------------
# 1. Download dataset from Kaggle
# ----------------------------------------
dataset = "eoinamoore/historical-nba-data-and-player-box-scores"
print("Downloading dataset: {} ...".format(dataset))
path = kagglehub.dataset_download(dataset)
print("Dataset located at:", path)

# ----------------------------------------
# 2. File paths
# ----------------------------------------
GAMES_FILE = os.path.join(path, "Games.csv")
PLAYER_STATS_FILE = os.path.join(path, "PlayerStatistics.csv")
TEAM_STATS_FILE = os.path.join(path, "TeamStatistics.csv")
TEAM_HISTORIES_FILE = os.path.join(path, "TeamHistories.csv")
PLAYERS_FILE = os.path.join(path, "Players.csv")

# ----------------------------------------
# 3. Helper function to load + filter
# ----------------------------------------
def load_and_filter(csv_path, datetime_col="gameDateTimeEst"):
    print("\nLoading {} ...".format(csv_path))
    df = pd.read_csv(csv_path)

    if datetime_col not in df.columns:
        raise Exception(
            "Column '{}' not found in {}. Available: {}".format(
                datetime_col, csv_path, list(df.columns)
            )
        )

    # Convert to datetime
    df[datetime_col] = pd.to_datetime(df[datetime_col], errors="coerce")

    # Filter
    cutoff = "2018-01-01"
    df = df[df[datetime_col] >= cutoff].copy()

    print("Filtered rows (>= {}): {}".format(cutoff, len(df)))
    return df

# ----------------------------------------
# 4. Load each dataset
# ----------------------------------------
df_games = load_and_filter(GAMES_FILE)
df_player_stats = load_and_filter(PLAYER_STATS_FILE)
df_team_stats = load_and_filter(TEAM_STATS_FILE)

# no filtering on these:
df_players = pd.read_csv(PLAYERS_FILE)
df_team_histories = pd.read_csv(TEAM_HISTORIES_FILE)

# ----------------------------------------
# 5. Load into PostgreSQL
# ----------------------------------------
PG_USER = "admin"
PG_PASSWORD = "admin123"
PG_HOST = "18.134.163.221"
PG_PORT = "5432"
PG_DB = "testdb"

conn_str = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
    PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, PG_DB
)

engine = create_engine(conn_str)

# Table names in Postgres
TABLE_GAMES = "games"
TABLE_PLAYER_STATS = "player_statistics"
TABLE_TEAM_STATS = "team_statistics"
TABLE_PLAYERS = "players"
TABLE_HISTORIES = "team_histories"

print("\nWriting to PostgreSQL...")

df_games.to_sql(TABLE_GAMES, engine, if_exists="append", index=False, chunksize=1000, method="multi")
print(" → {} rows → {}".format(len(df_games), TABLE_GAMES))

df_player_stats.to_sql(TABLE_PLAYER_STATS, engine, if_exists="append", index=False, chunksize=1000, method="multi")
print(" → {} rows → {}".format(len(df_player_stats), TABLE_PLAYER_STATS))

df_team_stats.to_sql(TABLE_TEAM_STATS, engine, if_exists="append", index=False, chunksize=1000, method="multi")
print(" → {} rows → {}".format(len(df_team_stats), TABLE_TEAM_STATS))

df_players.to_sql(TABLE_PLAYERS, engine, if_exists="append", index=False, chunksize=1000, method="multi")
print(" → {} rows → {}".format(len(df_players), TABLE_PLAYERS))

df_team_histories.to_sql(TABLE_HISTORIES, engine, if_exists="append", index=False, chunksize=1000, method="multi")
print(" → {} rows → {}".format(len(df_team_histories), TABLE_HISTORIES))

print("\n✔ All data loaded successfully.")
