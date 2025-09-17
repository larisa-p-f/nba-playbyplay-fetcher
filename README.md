# NBA Play-by-Play Data Fetcher

This project provides Python scripts to fetch **NBA play-by-play data** using the [nba_api](https://github.com/swar/nba_api).  
It retrieves **regular season** and **playoff** games, saves the full play-by-play logs, and extracts **foul events** into separate files.

---

## Features
- Fetch regular season and playoff games for any NBA season.
- Save **complete play-by-play logs** as CSV.
- Extract and save **fouls-only events** (where `EVENTMSGTYPE == 6`).
- Retry mechanism with exponential backoff for failed requests.
- Separate script to re-fetch failed games and append results.

---

## ⚙Requirements
- Python 3.8+
- Dependencies in `requirements.txt`

Install them with:
```bash
pip install -r requirements.txt
```

---

## Usage

### 1. Fetch all play-by-play data for a season
Set the `season` variable in `fetch_playbyplay.py` (e.g. `"2023-24"`) and run:
```bash
python fetch_playbyplay.py
```

This will generate:  
- **Full regular season data** → `data/playbyplay_regular_<season>.csv`  
- **Fouls (regular season)** → `data/foul_events_<season>_regular.csv`  
- **Full playoff data** → `data/playbyplay_playoffs_<season>.csv`  
- **Fouls (playoffs)** → `data/foul_events_<season>_playoff.csv`  
- **Failed game logs** → `data/failed_games_regular_<season>.txt` / `data/failed_playoff_games_<season>.txt`

---

### 2. Retry failed games
Run:
```bash
python fetch_failed_games.py
```

This will:  
- Retry games listed in the failed log file.  
- Append their data to the existing **full play-by-play CSV**.  
- Append their fouls to the **foul-only CSV**.  

---

## Example Outputs
- `data/playbyplay_regular_2023-24.csv` → All plays for regular season  
- `data/foul_events_2023-24_regular.csv` → Only foul plays for regular season  
- `data/playbyplay_playoffs_2023-24.csv` → All plays for playoffs  
- `data/foul_events_2023-24_playoff.csv` → Only foul plays for playoffs  

---

## Notes
- The script waits 5 seconds between requests to avoid hitting API rate limits.  
- If you want multiple seasons, update the `season` variable and re-run.  

---

## License
MIT License. Free to use and modify.
