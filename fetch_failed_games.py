import pandas as pd
from nba_api.stats.endpoints import playbyplayv2
from nba_api.stats.endpoints import leaguegamefinder
import os
import time
import requests
from fetch_playbyplay import get_playbyplay

# fetch filed regular season games, change the path if you need to fetch failed playoff games

season = "2023-24"

with open("data/failed_games_regular_"+season+".txt", "r") as f:
    failed_games = [line.strip() for line in f.readlines() if line.strip()]

print("Fetching failed regular season games...")
play_by_play_data = []

for game_id in failed_games:
    try:
        pbp = get_playbyplay(game_id, FAILED_LOG_PATH = "data/failed_games_regular_"+season+"_2.txt")
        if pbp.empty:
            continue  # if a game wasn't able to be fetched, skip it

        play_by_play_data.append(pbp)
    
    except Exception as e:
        print(f"Skipping regular season game {game_id} due to error: {e}")

if play_by_play_data:
    all_play_df = pd.concat(play_by_play_data, ignore_index=True)  # join all dataframes
    all_play_df.to_csv("data/playbyplay_regular_"+season+".csv", mode='a', index=False, header=False)

    # extract fouls and append to foul file
    reg_fouls_df = all_play_df[all_play_df['EVENTMSGTYPE'] == 6].copy()
    reg_fouls_df.to_csv("data/foul_events_"+season+"_regular.csv", mode='a', index=False, header=False)

    print(f"Data added to fetched games")

else:
    print("No new data fetched.")