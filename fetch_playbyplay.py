import pandas as pd
from nba_api.stats.endpoints import playbyplayv2
from nba_api.stats.endpoints import leaguegamefinder
import os
import time
import requests

def get_regular_games_for_season(season):
    """
    Fetch regular season NBA game IDs for a given season.
    Only includes games where home team data is available.

    Parameters:
        season_nullable (str): Season in 'YYYY-YY' format (e.g. '2018-19')

    Returns:
        DataFrame: Contains one column, 'GAME_ID', listing all valid games that season.
    """
    gamefinder = leaguegamefinder.LeagueGameFinder(player_or_team_abbreviation="T", 
                                                   season_type_nullable="Regular Season", 
                                                   season_nullable=season,
                                                   league_id_nullable="00")  # query for the games in regular season
    # the first dataframe of what is returned contains the data we want
    games_df = gamefinder.get_data_frames()[0]

    home_games = games_df[games_df['MATCHUP'].str.contains("vs\\.", regex=True)]  # filter MATCHUP for where the first team is the home team, written as _ vs. _ (instead of _ @ _ where the first team would be the away team)
    home_games = home_games.drop_duplicates(subset="GAME_ID")

    total_games = games_df['GAME_ID'].nunique()
    home_games_count = home_games['GAME_ID'].nunique()
    print(f"Dropped {total_games - home_games_count} games due to missing home team rows.")

    return home_games['GAME_ID'].tolist()

def get_playoff_games_for_season(season):
    """
    Fetch playoff NBA game IDs for a given season.
    Only includes games where home team data is available.

    Parameters:
        season_nullable (str): Season in 'YYYY-YY' format (e.g. '2018-19')

    Returns:
        DataFrame: Contains one column, 'GAME_ID', listing all valid playoff games that season.
    """
    gamefinder = leaguegamefinder.LeagueGameFinder(player_or_team_abbreviation="T", 
                                                   season_type_nullable="Playoffs", 
                                                   season_nullable=season,
                                                   league_id_nullable="00")
    games_df = gamefinder.get_data_frames()[0]

    home_games = games_df[games_df['MATCHUP'].str.contains("vs\\.", regex=True)]
    home_games = home_games.drop_duplicates(subset="GAME_ID")

    total_games = games_df['GAME_ID'].nunique()
    home_games_count = home_games['GAME_ID'].nunique()
    print(f"Dropped {total_games - home_games_count} playoff games due to missing home team rows.")

    return home_games['GAME_ID'].tolist()

def get_playbyplay(game_id, max_retries=5, backoff=5, FAILED_LOG_PATH = "data/failed_games.txt"):
    """
    Pull play-by-play data for a single game using its game ID.
    Retry fetching the game if there was a timeout error.

    Includes:
        - EVENTNUM: Event index
        - EVENTMSGTYPE: Event type (e.g. 6 = foul)
        - PERIOD: Quarter (1 = Q1, 5 = OT1, etc.)
        - PCTIMESTRING: Time left in quarter
        - HOMEDESCRIPTION / VISITORDESCRIPTION: Text commentary
        - SCOREMARGIN: Current score margin

    Parameters:
        game_id (str): Game ID like '0021800854'
        max_retries (int): Number of times to try fetching the same game.
        backoff (int): Exponentially increases the wait between fetching a failed game.

    Returns:
        DataFrame: Subset of play-by-play data with key columns.
    """

    for attempt in range(max_retries):
        try:
            time.sleep(5)  # prevent triggering rate limiting
            df = playbyplayv2.PlayByPlayV2(game_id = game_id).get_data_frames()[0]
            df['SCOREMARGIN'] = df['SCOREMARGIN'].ffill()  # score margin are NaN except for when a basket is made, forward fill NaN values with latest score margin value
            df['SCOREMARGIN'] = df['SCOREMARGIN'].replace(to_replace='TIE', value=0)  # replace TIE with 0
            df['SCOREMARGIN'] = pd.to_numeric(df['SCOREMARGIN'])  # make the column numeric
            df['SCOREMARGIN'] = df['SCOREMARGIN'].fillna(0)  # replace NaN with 0
            print(f"Successfully fetched play-by-play for game {game_id}.")
            return df
        
        except requests.exceptions.ReadTimeout:
            print(f"Timeout on game {game_id}, attempt {attempt + 1}/{max_retries}")
            time.sleep(backoff * (attempt + 1))  # exponential backoff
        except Exception as e:
            print(f"Other error on game {game_id}: {e}")
            return pd.DataFrame()  # return empty so concat doesn't fail

    print(f"Failed to get play-by-play for game {game_id} after {max_retries} retries.")
    with open(FAILED_LOG_PATH, "a") as f:
        f.write(f"{game_id}\n")
    return pd.DataFrame()  # return empty dataframe, avoids crashing the script

def main():
    season = "2023-24"
    print('Gathering data for the {} NBA season.'.format(season))

    os.makedirs("data", exist_ok=True)

    # regular season
    print("Fetching regular season games...")
    play_by_play_data = []
    game_ids = get_regular_games_for_season(season)

    for game_id in game_ids:
        try:
            pbp = get_playbyplay(game_id, FAILED_LOG_PATH = "data/failed_games_regular_"+season+".txt")
            if pbp.empty:
                continue  # if a game wasn't able to be fetched, skip it

            play_by_play_data.append(pbp)
        
        except Exception as e:
            print(f"Skipping regular season game {game_id} due to error: {e}")

    # combine all play by play data into one dataframe
    all_pbp_df = pd.concat(play_by_play_data, ignore_index=True)

    # save data
    out_path = f"data/playbyplay_regular_{season.replace('/', '-')}.csv"
    all_pbp_df.to_csv(out_path, index=False)
    print(f"Saved regular season play-by-play data to {out_path}")

    # extract fouls into separate file
    reg_fouls_df = all_pbp_df[all_pbp_df['EVENTMSGTYPE'] == 6].copy()
    reg_foul_out_path = f"data/foul_events_{season.replace('/', '-')}_regular.csv"
    reg_fouls_df.to_csv(reg_foul_out_path, index=False)
    print(f"Also saved fouls subset to {reg_foul_out_path}")

    # playoffs
    print("Fetching playoff games...")
    playoff_game_ids = get_playoff_games_for_season(season)
    playoff_pbp_data = []

    for game_id in playoff_game_ids:
        try:
            pbp = get_playbyplay(game_id, FAILED_LOG_PATH=f"data/failed_playoff_games_{season}.txt")
            if pbp.empty:
                continue
            playoff_pbp_data.append(pbp)
        except Exception as e:
            print(f"Skipping playoff game {game_id} due to error: {e}")

    playoff_df = pd.concat(playoff_pbp_data, ignore_index=True)
    playoff_out_path = f"data/playbyplay_playoffs_{season.replace('/', '-')}.csv"
    playoff_df.to_csv(playoff_out_path, index=False)
    print(f"Saved playoff play-by-play data to {playoff_out_path}")

    # extract playoff fouls into separate file
    pl_fouls_df = playoff_df[playoff_df['EVENTMSGTYPE'] == 6].copy()
    playoff_foul_out_path = f"data/foul_events_{season.replace('/', '-')}_playoff.csv"
    pl_fouls_df.to_csv(playoff_foul_out_path, index=False)
    print(f"Also saved fouls subset to {playoff_foul_out_path}")

if __name__ == '__main__':
    main()

