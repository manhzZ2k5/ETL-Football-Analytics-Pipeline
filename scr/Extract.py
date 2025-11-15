from pathlib import Path
import soccerdata as sd
import pandas as pd
import os

# Xác định BASE_DIR: ưu tiên environment variable, nếu không thì dùng relative path từ script
if "ETL_FOOTBALL_BASE_DIR" in os.environ:
    BASE_DIR = os.environ["ETL_FOOTBALL_BASE_DIR"]
else:
    # Lấy thư mục chứa script (scr/), rồi lên 1 level để có BASE_DIR
    BASE_DIR = str(Path(__file__).parent.parent.absolute())

# Thư mục lưu raw data
DATA_RAW_DIR = os.path.join(BASE_DIR, "data_raw")

# Tạo thư mục nếu chưa tồn tại
os.makedirs(DATA_RAW_DIR, exist_ok=True)

def main():
    print("=" * 60)
    print("ETL Football - Extract Process")
    print("=" * 60)
    print(f"BASE_DIR: {BASE_DIR}")
    print(f"DATA_RAW_DIR: {DATA_RAW_DIR}")
    print("=" * 60)
    
    fbref = sd.FBref(
        leagues="ENG-Premier League",
        seasons=['2021', '2122', '2223', '2324', '2425']
    )
    
    player_season = fbref.read_player_season_stats()

    player_season_df = pd.DataFrame(player_season)
    # Reset index để chuyển league, season, team, player từ index thành cột
    player_season_df = player_season_df.reset_index()
    
    out_path = Path(DATA_RAW_DIR) / "fbref_fact_player_season_stats.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    player_season_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")
    

    print("Fetching player match statistics...")
    player_match = fbref.read_player_match_stats()
    player_match_df = pd.DataFrame(player_match)

    player_match_df = player_match_df.reset_index()
    
    out_path = Path(DATA_RAW_DIR) / "fbref_fact_player_match_stats.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    player_match_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")

    print("Fetching team match statistics...")
    team_match = fbref.read_team_match_stats()
    team_match_df = pd.DataFrame(team_match)
    team_match_df = team_match_df.reset_index()
    
    out_path = Path(DATA_RAW_DIR) / "fbref_fact_team_match.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    team_match_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")
    

    print("Fetching team season statistics...")
    team_season = fbref.read_team_season_stats()
    team_season_df = pd.DataFrame(team_season)
    team_season_df = team_season_df.reset_index()
    
    out_path = Path(DATA_RAW_DIR) / "fbref_fact_team_season.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    team_season_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")
    

    print("Fetching Understat shot events...")
    understat = sd.Understat(
        leagues="ENG-Premier League",
        seasons=['2021','2122','2223','2324','2425']
    )
    shot = understat.read_shot_events()
    shot_df = pd.DataFrame(shot)
    
    out_path = Path(DATA_RAW_DIR) / "understat_fact_shot.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    shot_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")
    
    print("successfully")


if __name__ == "__main__":
    main()
