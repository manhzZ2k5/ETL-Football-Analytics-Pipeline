from pathlib import Path
import soccerdata as sd
import pandas as pd
import os
import numpy as np
from time import sleep
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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

def scrape_team_points():
    """Scrape team standings/points from flashscore.com và lưu vào team_point.csv"""
    print("Fetching team points/standings from flashscore.com...")

    # Cấu hình Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Tự động tải ChromeDriver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    try:
        # Danh sách 5 mùa gần nhất
        seasons = ["2024-2025", "2023-2024", "2022-2023", "2021-2022", "2020-2021"]
        
        # Tạo list để lưu kết quả
        all_data = []
        
        # Duyệt qua từng mùa
        for season in seasons:
            print(f"Processing season: {season}")
            base_url = f"https://www.flashscore.com/football/england/premier-league-{season}/#/lAkHuyP3/standings/"
            
            # Duyệt qua 3 loại bảng (overall, home, away)
            categories = ["overall", "home", "away"]
            
            for cat in categories:
                url = f"{base_url}{cat}/"
                driver.get(url)
                sleep(2.5)
                
                # Lấy mùa giải hiển thị trên trang
                try:
                    season_label = driver.find_element(By.CSS_SELECTOR, "div.heading__info").text.strip()
                except:
                    season_label = season
                
                # Lấy dữ liệu
                ranks = [r.text.strip() for r in driver.find_elements(By.CSS_SELECTOR, "div.tableCellRank")]
                teams = [t.text.strip() for t in driver.find_elements(By.CSS_SELECTOR, "a.tableCellParticipant__name")]
                values = [v.text.strip() for v in driver.find_elements(By.CSS_SELECTOR, "span.table__cell.table__cell--value")]
                
                # Lấy form gần đây
                forms_all = driver.find_elements(By.CSS_SELECTOR, "div.table__cell.table__cell--form")
                recent_forms = []
                for form_cell in forms_all:
                    results = [s.text.strip() for s in form_cell.find_elements(By.CSS_SELECTOR, "span.wcl-scores-simple-text-01_8lVyp")]
                    recent_forms.append("".join(results))
                
                # Chia giá trị theo hàng (mỗi hàng có 7 giá trị: MP, W, D, L, GF:GA, GD, Pts)
                rows = [values[i:i + 7] for i in range(0, len(values), 7)]
                
                # Gộp dữ liệu
                for i, row in enumerate(rows):
                    all_data.append({
                        "Mùa giải": season_label,
                        "Match_Category": cat,
                        "Rank": ranks[i] if i < len(ranks) else "",
                        "Team": teams[i] if i < len(teams) else "",
                        "MP": row[0] if len(row) > 0 else "",
                        "W": row[1] if len(row) > 1 else "",
                        "D": row[2] if len(row) > 2 else "",
                        "L": row[3] if len(row) > 3 else "",
                        "GF:GA": row[4] if len(row) > 4 else "",
                        "GD": row[5] if len(row) > 5 else "",
                        "Pts": row[6] if len(row) > 6 else "",
                        "Recent_Form": recent_forms[i] if i < len(recent_forms) else ""
                    })
        
        # Chuyển đổi sang DataFrame và lưu vào CSV
        columns = ["Mùa giải", "Match_Category", "Rank", "Team", "MP", "W", "D", "L", "GF:GA", "GD", "Pts", "Recent_Form"]
        team_points_df = pd.DataFrame(all_data, columns=columns)
        
        out_path = Path(DATA_RAW_DIR) / "team_point.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        team_points_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"Saved: {out_path}")
        print(f"Total records: {len(team_points_df)}")
        
    except Exception as e:
        print(f"Error scraping team points: {e}")
        raise
    finally:
        driver.quit()

def main():

    fbref = sd.FBref(
        leagues="ENG-Premier League",
        seasons=['2021', '2122', '2223', '2324', '2425']
    )
    
    player_season = fbref.read_player_season_stats()

    player_season_df = pd.DataFrame(player_season)
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
    
    scrape_team_points()
    
    print("successfully")


if __name__ == "__main__":
    main()
