"""
ETL Script: Fetch Football Data from Multiple Sources
- FBref: Player & Team Statistics
- Understat: Shot Events
"""

from pathlib import Path
import soccerdata as sd
import pandas as pd


def main():

    fbref = sd.FBref(
        leagues="ENG-Premier League",
        seasons=['2021', '2122', '2223', '2324', '2425']
    )
    
    player_season = fbref.read_player_season_stats()
    player_season_df = pd.DataFrame(player_season)
    # Reset index để chuyển league, season, team, player từ index thành cột
    player_season_df = player_season_df.reset_index()
    
    out_path = Path("../data/fbref_fact_player_season_stats.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    player_season_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")
    

    print("Fetching player match statistics...")
    player_match = fbref.read_player_match_stats()
    player_match_df = pd.DataFrame(player_match)
    # Reset index để chuyển league, season, game, team, player từ index thành cột
    player_match_df = player_match_df.reset_index()
    
    out_path = Path("../data/fbref_fact_player_match_stats.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    player_match_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")

    print("Fetching team match statistics...")
    team_match = fbref.read_team_match_stats()
    team_match_df = pd.DataFrame(team_match)
    # Reset index để chuyển league, season, team, game từ index thành cột
    team_match_df = team_match_df.reset_index()
    
    out_path = Path("../data/fbref_fact_team_match.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    team_match_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")
    

    print("Fetching team season statistics...")
    team_season = fbref.read_team_season_stats()
    team_season_df = pd.DataFrame(team_season)
    # Reset index để chuyển league, season, team từ index thành cột
    team_season_df = team_season_df.reset_index()
    
    out_path = Path("../data/fbref_fact_team_season.csv")
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
    
    out_path = Path("../data/understat_fact_shot.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    shot_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")
    
    print("\n✨ ETL pipeline completed successfully!")


if __name__ == "__main__":
    main()
