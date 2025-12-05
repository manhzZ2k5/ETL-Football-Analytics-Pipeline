from pathlib import Path
import soccerdata as sd
import pandas as pd
import os
import numpy as np
from time import sleep
import random
import warnings
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

if "ETL_FOOTBALL_BASE_DIR" in os.environ:
    BASE_DIR = os.environ["ETL_FOOTBALL_BASE_DIR"]
else:
    BASE_DIR = str(Path(__file__).parent.parent.absolute())

DATA_RAW_DIR = os.path.join(BASE_DIR, "data_raw")

# Tạo thư mục nếu chưa tồn tại
os.makedirs(DATA_RAW_DIR, exist_ok=True)


def flatten_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Chuẩn hoá column names thành single-level để tránh ghi file với multi-index headers.
    """
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        flattened = []
        for idx, col in enumerate(df.columns):
            parts = []
            for level in col:
                if level is None:
                    continue
                level_str = str(level).strip()
                if not level_str or level_str.startswith("Unnamed"):
                    continue
                parts.append(level_str)
            if not parts:
                parts = [f"column_{idx}"]
            flattened.append("_".join(parts))
        df.columns = flattened
    else:
        df.columns = [str(col).strip() for col in df.columns]
    return df


def read_existing_raw_file(file_path: str) -> pd.DataFrame:
    """
    Đọc dữ liệu raw và xử lý các trường hợp file có multi-level header.
    Luôn trả về DataFrame với single-level columns.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", pd.errors.DtypeWarning)
        df = pd.read_csv(file_path, dtype=str, low_memory=False)
    # Nếu header bị duplicated (multi-level) -> đọc lại với header=[0,1]
    if df.columns.duplicated().any():
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", pd.errors.DtypeWarning)
            df = pd.read_csv(file_path, header=[0, 1], dtype=str, low_memory=False)
    return flatten_dataframe_columns(df)


def scrape_team_points(seasons_to_scrape=None):
  
    print("Scraping team points from flashscore.com...")
 
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--remote-debugging-port=9222")
    # Thêm user-agent để tránh bị chặn bởi Flashscore
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
    
    # Tự động phát hiện Chrome binary path trong Docker
    if os.path.exists("/usr/bin/google-chrome"):
        chrome_options.binary_location = "/usr/bin/google-chrome"
    elif os.path.exists("/usr/bin/google-chrome-stable"):
        chrome_options.binary_location = "/usr/bin/google-chrome-stable"

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # --- CẤU HÌNH ID MÙA GIẢI (QUAN TRỌNG: FIX LỖI DÒNG 89) ---
    # Map tên mùa giải sang ID của Flashscore/Flashfootball
    SEASON_IDS = {
        "2025-2026": "OEEq9Yvp",
        "2024-2025": "lAkHuyP3",
        "2023-2024": "I3O5jpB2",
        "2022-2023": "nunhS7Vn",
        "2021-2022": "6kJqdMr2",
        "2020-2021": "zTRyeuJg"
    }

    try:
        # Xác định mùa giải cần scrape
        if seasons_to_scrape is None:
            # Kiểm tra xem có dữ liệu cũ không
            LAST_EXTRACT_DATE_FILE = os.path.join(DATA_RAW_DIR, ".last_extract_date.txt")
            has_existing_data = False
            if os.path.exists(LAST_EXTRACT_DATE_FILE):
                try:
                    with open(LAST_EXTRACT_DATE_FILE, 'r') as f:
                        if f.read().strip():
                            has_existing_data = True
                except:
                    pass
            
            if has_existing_data:
                seasons = ["2025-2026"]  # Chỉ   mùa hiện tại nếu đã có dữ liệu cũ
                print(f"Scraping NEW season: {seasons}")
            else:
                seasons = ["2025-2026","2024-2025", "2023-2024", "2022-2023", "2021-2022", "2020-2021"]
                print(f"First time scraping ALL seasons: {seasons}")
        else:
            seasons = seasons_to_scrape
            print(f"Scraping specified seasons: {seasons}")
        
        all_data = []
        for season in seasons:
            # Kiểm tra xem mùa giải có ID trong config không
            if season not in SEASON_IDS:
                print(f"Warning: No ID found for season {season}, skipping...")
                continue

            season_id = SEASON_IDS[season]
            print(f"Processing season: {season} (ID: {season_id})")
            
            # --- URL ĐÃ ĐƯỢC SỬA ---
            # Sử dụng ID động thay vì hardcode
            base_url = f"https://www.flashfootball.com/england/premier-league-{season}/#/{season_id}/standings/"
            
            categories = ["overall", "home", "away"]
            
            for cat in categories:
                url = f"{base_url}{cat}/"
                driver.get(url)
                
                # Tăng wait time và thêm explicit wait cho table
                print(f"  Loading {cat} standings...")
                sleep(3) 
                
                # Wait cho table load xong
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.tableCellRank"))
                    )
                    sleep(1)
                except:
                    print(f"  Warning: Table not fully loaded for {cat}")
                
                # Lấy dữ liệu
                # Note: Sử dụng các selector phổ biến của Flashscore/Flashfootball
                ranks = [r.text.strip() for r in driver.find_elements(By.CSS_SELECTOR, "div.tableCellRank")]
                teams = [t.text.strip() for t in driver.find_elements(By.CSS_SELECTOR, "a.tableCellParticipant__name")]
                values = [v.text.strip() for v in driver.find_elements(By.CSS_SELECTOR, "span.table__cell.table__cell--value")]
                
                forms_all = driver.find_elements(By.CSS_SELECTOR, "div.table__cell.table__cell--form")
                recent_forms = []
                for form_cell in forms_all:
                    # Lấy text form (W/D/L)
                    results = [s.text.strip() for s in form_cell.find_elements(By.CSS_SELECTOR, "[class*='wcl-scores-simple-text']")]
                    form_str = "".join(results)
                    # Clean dấu "?" và các ký tự đặc biệt khác
                    form_str = form_str.replace("?", "").strip()
                    recent_forms.append(form_str)
                
                # Chia giá trị theo hàng (mỗi hàng có 7 giá trị: MP, W, D, L, GF:GA, GD, Pts)
                rows = [values[i:i + 7] for i in range(0, len(values), 7)]
                
                # Verify data quality - log first team để check
                if len(teams) > 0 and len(rows) > 0:
                    print(f"  {cat.upper()}: {teams[0]} - MP:{rows[0][0]} W:{rows[0][1]} GF:GA:{rows[0][4]} Form:{recent_forms[0] if recent_forms else 'N/A'}")
                
                for i, row in enumerate(rows):
                    if i < len(teams):
                        all_data.append({
                            "Mùa giải": season, # Dùng biến loop season để đảm bảo chuẩn format
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
        
        columns = ["Mùa giải", "Match_Category", "Rank", "Team", "MP", "W", "D", "L", "GF:GA", "GD", "Pts", "Recent_Form"]
        team_points_df = pd.DataFrame(all_data, columns=columns)
        
        out_path = Path(DATA_RAW_DIR) / "team_point.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Merge với dữ liệu cũ nếu có
        team_points_df = merge_with_existing_raw_data(
            team_points_df,
            out_path,
            ["Mùa giải", "Match_Category", "Team"]  # Key columns
        )
        
        team_points_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"Saved: {out_path} ({len(team_points_df)} records)")
        
    except Exception as e:
        print(f"Error scraping: {e}")
        raise
    finally:
        driver.quit()

def get_seasons_to_extract():
    """
    - Nếu chưa có dữ liệu: extract tất cả từ 2021 (5 mùa)
    - Nếu đã có dữ liệu: chỉ extract mùa mới từ 2024-25 trở đi
    """
    LAST_EXTRACT_DATE_FILE = os.path.join(DATA_RAW_DIR, ".last_extract_date.txt")
    
    # Kiểm tra xem đã có dữ liệu chưa
    has_existing_data = False
    if os.path.exists(LAST_EXTRACT_DATE_FILE):
        try:
            with open(LAST_EXTRACT_DATE_FILE, 'r') as f:
                date_str = f.read().strip()
                if date_str:
                    has_existing_data = True
                    print(f"Last extract date found: {date_str}")
        except:
            pass
    
    if has_existing_data:
        # Chỉ cào mùa giải mới nhất 2425 (đã có mùa trước rồi)
        print("Extracting NEW seasons only (2024-25)...")
        return ['2425'] # Đã sửa thành 2425 vì mùa 2526 chưa có
    else:
        # cào 5 mùa
        print("First time extraction: extracting ALL seasons (2020-21 to 2024-25)...")
        return ['2021', '2122', '2223', '2324', '2425']


def save_last_extract_date():
    "Lưu ngày extract cuối cùng"
    from datetime import datetime
    LAST_EXTRACT_DATE_FILE = os.path.join(DATA_RAW_DIR, ".last_extract_date.txt")
    
    today = datetime.now().date().strftime('%Y-%m-%d')
    with open(LAST_EXTRACT_DATE_FILE, 'w') as f:
        f.write(today)
    print(f"Saved: {today}")


def merge_with_existing_raw_data(new_df, file_path, key_cols):
    """
    Merge dữ liệu mới với dữ liệu cũ trong file raw (incremental extract).
    """
    new_df = flatten_dataframe_columns(new_df)

    if not os.path.exists(file_path):
        print(f"  First time: No existing data to merge")
        return new_df
    
    try:
        # Đọc dữ liệu cũ
        old_df = read_existing_raw_file(file_path)
        if old_df.empty:
            print(f"  Existing file is empty, using new data only")
            return new_df
        
        # Kiểm tra columns có khớp không
        if set(old_df.columns) != set(new_df.columns):
            print(f"  Warning: Columns mismatch, using new data only")
            return new_df
        
        # Merge: loại bỏ records cũ có cùng key với records mới
        if key_cols and all(col in old_df.columns for col in key_cols):
            # Lấy keys của dữ liệu mới
            new_keys = set(new_df[key_cols].apply(tuple, axis=1))
            # Lọc dữ liệu cũ: loại bỏ những records có key trùng với dữ liệu mới
            old_df_filtered = old_df[~old_df[key_cols].apply(tuple, axis=1).isin(new_keys)]
            # Merge: dữ liệu cũ (đã filter) + dữ liệu mới
            combined = pd.concat([old_df_filtered, new_df], ignore_index=True)
            print(f"  Merged: {len(old_df)} old - {len(old_df) - len(old_df_filtered)} duplicates + {len(new_df)} new = {len(combined)} total")
            return combined
        else:
            # Nếu không có key_cols, chỉ append và drop_duplicates
            combined = pd.concat([old_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(keep='last')
            print(f"  Merged: {len(old_df)} old + {len(new_df)} new = {len(combined)} total (after dedup)")
            return combined
    except Exception as e:
        print(f"  Warning: Could not merge with existing data: {e}")
        print(f"  Using new data only")
        return new_df


def main():
    # Apply bypass headers globally
    try:
        from bypass_headers import apply_bypass
        apply_bypass()
    except ImportError:
        print("[WARN] bypass_headers not found, using default requests")
    
    # Xác định mùa giải cần extract
    seasons_to_extract = get_seasons_to_extract()
    print(f"Seasons to extract: {seasons_to_extract}")
    
    # Create FBref instance
    fbref = sd.FBref(
        leagues="ENG-Premier League",
        seasons=seasons_to_extract
    )
    
    # ===== PLAYER SEASON STATS =====
    print("\n1. Fetching player season statistics...")
    player_season = fbref.read_player_season_stats()
    player_season_df = pd.DataFrame(player_season)
    player_season_df = player_season_df.reset_index()
    
    out_path = Path(DATA_RAW_DIR) / "fbref_fact_player_season_stats.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Merge với dữ liệu cũ nếu có
    player_season_df = merge_with_existing_raw_data(
        player_season_df, 
        out_path, 
        ['season', 'player', 'team']  # Key columns để identify duplicates
    )
    
    player_season_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path} ({len(player_season_df)} records)")
    

    print("\n2. Fetching player match statistics...")
    player_match = fbref.read_player_match_stats()
    player_match_df = pd.DataFrame(player_match)
    player_match_df = player_match_df.reset_index()
    
    out_path = Path(DATA_RAW_DIR) / "fbref_fact_player_match_stats.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Merge với dữ liệu cũ nếu có
    player_match_df = merge_with_existing_raw_data(
        player_match_df,
        out_path,
        ['season', 'game', 'player', 'team']  # Key columns
    )
    
    player_match_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path} ({len(player_match_df)} records)")

    # ===== TEAM MATCH STATS =====
    print("\n3. Fetching team match statistics...")
    team_match = fbref.read_team_match_stats()
    team_match_df = pd.DataFrame(team_match)
    team_match_df = team_match_df.reset_index()
    
    out_path = Path(DATA_RAW_DIR) / "fbref_fact_team_match.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Merge với dữ liệu cũ nếu có
    team_match_df = merge_with_existing_raw_data(
        team_match_df,
        out_path,
        ['season', 'game', 'team']  # Key columns
    )
    
    team_match_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path} ({len(team_match_df)} records)")
    

    # ===== TEAM SEASON STATS =====
    print("\n4. Fetching team season statistics...")
    team_season = fbref.read_team_season_stats()
    team_season_df = pd.DataFrame(team_season)
    team_season_df = team_season_df.reset_index()
    
    out_path = Path(DATA_RAW_DIR) / "fbref_fact_team_season.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Merge với dữ liệu cũ nếu có
    team_season_df = merge_with_existing_raw_data(
        team_season_df,
        out_path,
        ['season', 'team']  # Key columns
    )
    
    team_season_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path} ({len(team_season_df)} records)")
    

    # ===== FLASHSCORE SCRAPING =====
    print("\n6. Scraping team points from Flashscore/Flashfootball...")
    
    # Lưu ý: Hàm này tự quản lý list mùa giải bên trong nó (dựa trên việc có data cũ hay chưa)
    # Nếu bạn muốn force chạy 5 mùa, hãy xoá file .last_extract_date.txt trước khi chạy
    scrape_team_points()
    
    # Lưu ngày extract cuối cùng
    save_last_extract_date()
    
    print("Extract completed")


if __name__ == "__main__":
    main()