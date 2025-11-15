import pandas as pd
import numpy as np
import os
from pathlib import Path

# Xác định BASE_DIR: ưu tiên environment variable, nếu không thì dùng relative path từ script
# Script nằm trong scr/, nên BASE_DIR là parent directory
if "ETL_FOOTBALL_BASE_DIR" in os.environ:
    BASE_DIR = os.environ["ETL_FOOTBALL_BASE_DIR"]
else:
    # Lấy thư mục chứa script (scr/), rồi lên 1 level để có BASE_DIR
    BASE_DIR = str(Path(__file__).parent.parent.absolute())

# Thư mục chứa raw data
DATA_DIR = os.path.join(BASE_DIR, "data_raw")
DATA_PROCESSED_DIR = os.path.join(BASE_DIR, "data_processed")

# Tạo thư mục nếu chưa tồn tại
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)


def create_dim_player():
    print("Creating dim_player:")
    
    df = pd.read_csv(
        os.path.join(DATA_DIR, "fbref_fact_player_season_stats.csv"),
        header=[0, 1, 2]
    )
    
    # Tìm cột trong MultiIndex: header=[0,1,2] tạo 3 levels (level0, level1, level2)
    # Level 0: tên cột ('player', 'pos', 'nation', 'born')
    # Level 1: Unnamed: X_level_1 (từ header row 2)
    # Level 2: giá trị dữ liệu từ dòng đầu (thay đổi, không dùng để match)
    # Ví dụ: 'player', 'Unnamed: 3_level_1', 'Ainsley Maitland-Niles'
    # match level 0 và level 1 để tìm cột
    player_col = [col for col in df.columns if col[0] == 'player' and col[1] == 'Unnamed: 3_level_1'][0]
    pos_col = [col for col in df.columns if col[0] == 'pos' and col[1] == 'Unnamed: 5_level_1'][0]
    nation_col = [col for col in df.columns if col[0] == 'nation' and col[1] == 'Unnamed: 4_level_1'][0]
    born_col = [col for col in df.columns if col[0] == 'born' and col[1] == 'Unnamed: 7_level_1'][0]
    
    df_subset = df[[player_col, pos_col, nation_col, born_col]].copy()
    
    df_subset.columns = ['player', 'pos', 'nation', 'born']
    
    df_subset = df_subset.drop_duplicates(subset='player', keep='first')
    
    # Thiết lập lại index để player_id liên tiếp
    df_subset = df_subset.reset_index(drop=True)
    
    # Tạo player_id tăng dần từ 1
    df_subset['player_id'] = np.arange(len(df_subset)) + 1
    
    df_subset['born'] = df_subset['born'].astype('Int64')
    df_subset = df_subset[['player_id', 'player', 'pos', 'nation', 'born']]

    output_path = os.path.join(DATA_PROCESSED_DIR, "dim_player.csv")
    df_subset.to_csv(output_path, index=False)
    print(f"dim_player created: {len(df_subset)} records")
    return df_subset


def create_dim_team():
    print("Creating dim_team:")
    df = pd.read_csv(os.path.join(DATA_DIR, "dim_team.csv"))

    # Loại bỏ dòng header trùng lặp 
    header_row = list(df.columns)
    df = df[~df.apply(lambda row: list(row.values) == header_row, axis=1)].reset_index(drop=True)
    
    df_subset = df[['club_id', 'club_label', 'founding_year', 'venue_id']].copy()
    df_subset.columns = ['team_id', 'team_name', 'founded_year', 'stadium_id']
    
    name_map = {
        "AFC Bournemouth": "BOU",
        "Arsenal F.C.": "ARS",
        "Aston Villa F.C.": "AVL",
        "Brentford F.C.": "BRE",
        "Brighton & Hove Albion F.C.": "BHA",
        "Chelsea F.C.": "CHE",
        "Crystal Palace F.C.": "CRY",
        "Everton F.C.": "EVE",
        "Fulham F.C.": "FUL",
        "Ipswich Town F.C.": "IPS",
        "Leicester City F.C.": "LEI",
        "Liverpool F.C.": "LIV",
        "Manchester City F.C.": "MCI",
        "Manchester United F.C.": "MUN",
        "Newcastle United F.C.": "NEW",
        "Nottingham Forest F.C.": "NOT",
        "Southampton F.C.": "SOU",
        "Tottenham Hotspur F.C.": "TOT",
        "West Ham United F.C.": "WHU",
        "Wolverhampton Wanderers F.C.": "WOL",
        "Blackburn Rovers F.C.": "BLA",
        "Bristol City F.C.": "BRC",
        "Burnley F.C.": "BUR",
        "Cardiff City F.C.": "CAR",
        "Coventry City F.C.": "COV",
        "Derby County F.C.": "DER",
        "Hull City A.F.C.": "HUL",
        "Leeds United F.C.": "LEE",
        "Luton Town F.C.": "LUT",
        "Middlesbrough F.C.": "MID",
        "Millwall F.C.": "MIL",
        "Norwich City F.C.": "NOR",
        "Oxford United F.C.": "OXF",
        "Plymouth Argyle F.C.": "PLY",
        "Portsmouth F.C.": "POR",
        "Preston North End F.C.": "PNE",
        "Queens Park Rangers F.C.": "QPR",
        "Sheffield United F.C.": "SHU",
        "Sheffield Wednesday F.C.": "SHW",
        "Stoke City F.C.": "STK",
        "Sunderland A.F.C.": "SUN",
        "Swansea City A.F.C.": "SWA",
        "Watford F.C.": "WAT",
        "West Bromwich Albion F.C.": "WBA"
    }
    
    df_subset["short_name"] = df_subset["team_name"].replace(name_map)
    
    remove_words = ["F.C.", "F.C", "FC", "AFC", "A.F.C.", "A.F.C"]
    
    def clean_team_name(name):
        for w in remove_words:
            name = name.replace(w, "")
        return name.strip()
    
    df_subset["team_name"] = df_subset["team_name"].apply(clean_team_name)

    name_map = {
        "Brighton & Hove Albion": "Brighton",
        "Manchester United": "Manchester utd",
        "Newcastle United": "Newcastle utd",
        "Sheffield United": "Sheffield utd",
        "Tottenham Hotspur": "Tottenham",
        "West Bromwich Albion": "West brom",
        "West Ham United": "West ham",
        "Wolverhampton Wanderers": "Wolves",
        "A Bournemouth": "Bournemouth",
        "Nottingham Forest": "Nott'ham forest"
    }
    df_subset["team_name"] = df_subset["team_name"].replace(name_map)
    
    # Loại bỏ "Q" và chuyển sang integer
    df_subset["team_id"] = df_subset["team_id"].astype(str).str.replace("Q", "", regex=False)
    df_subset["team_id"] = pd.to_numeric(df_subset["team_id"], errors='coerce').astype('Int64')
    
    df_subset["stadium_id"] = df_subset["stadium_id"].astype(str).str.replace("Q", "", regex=False)
    df_subset["stadium_id"] = pd.to_numeric(df_subset["stadium_id"], errors='coerce').astype('Int64')
    
    output_path = os.path.join(DATA_PROCESSED_DIR, "dim_team.csv")
    df_subset.to_csv(output_path, index=False)
    print(f"dim_team created: {len(df_subset)} records")
    return df_subset


def create_dim_stadium():
    print("Creating dim_stadium:")
    
    df = pd.read_csv(os.path.join(DATA_DIR, "dim_team.csv"))
    
    header_row = list(df.columns)
    df = df[~df.apply(lambda row: list(row.values) == header_row, axis=1)].reset_index(drop=True)
    
    df_subset = df[['venue_id', 'venue_label', 'capacity']].copy()
    df_subset.columns = ['stadium_id', 'statium_name', 'capacity']
    
    # Loại bỏ dòng có giá trị "capacity" trong cột capacity (nếu còn)
    df_subset = df_subset[df_subset['capacity'].astype(str).str.lower() != 'capacity'].reset_index(drop=True)
    
    # Loại bỏ "Q" và chuyển stadium_id sang integer
    df_subset['stadium_id'] = df_subset['stadium_id'].astype(str).str.replace('Q', '', regex=False)
    df_subset['stadium_id'] = pd.to_numeric(df_subset['stadium_id'], errors='coerce').astype('Int64')
    
    # Chuyển capacity sang integer
    df_subset['capacity'] = pd.to_numeric(df_subset['capacity'], errors='coerce')
    df_subset = df_subset.dropna(subset=['capacity'])
    df_subset['capacity'] = df_subset['capacity'].astype(int)
    
    output_path = os.path.join(DATA_PROCESSED_DIR, "dim_stadium.csv")
    df_subset.to_csv(output_path, index=False)
    print(f"dim_stadium created: {len(df_subset)} records")
    return df_subset


def create_fact_team_match():
    print("Creating fact_team_match_clean:")
    
    df = pd.read_csv(os.path.join(DATA_DIR, "fbref_fact_team_match.csv"))
    df_team = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_team.csv'))
    df_match = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_match.csv'))
    df_player = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_player.csv'))
    
    # CHUẨN HÓA CHUỖI
    df['team'] = df['team'].astype(str).str.strip().str.lower()
    df_team['team_name'] = df_team['team_name'].astype(str).str.strip().str.lower()
    
    df['game'] = df['game'].astype(str).str.strip().str.lower()
    df_match['game'] = df_match['game'].astype(str).str.strip().str.lower()
    
    df['Captain'] = df['Captain'].astype(str).str.strip().str.lower()
    df_player['player'] = df_player['player'].astype(str).str.strip().str.lower()
    
    df['opponent'] = df['opponent'].astype(str).str.strip().str.lower()
    
    # MAP TEAM → team_id
    df = df.merge(
        df_team[['team_id', 'team_name']].rename(columns={'team_name': 'team'}),
        on='team',
        how='left'
    )
    
    # MAP OPPONENT → opponent_id
    df = df.merge(
        df_team[['team_id', 'team_name']].rename(columns={'team_name': 'opponent'}),
        on='opponent',
        how='left',
        suffixes=('', '_opp')
    )
    df.rename(columns={'team_id_opp': 'opponent_id'}, inplace=True)
    
    # MAP GAME → game_id
    df = df.merge(
        df_match[['game_id', 'game']],
        on='game',
        how='left'
    )
    
    # MAP CAPTAIN → captain_id
    df = df.merge(
        df_player[['player_id', 'player']],
        left_on='Captain',
        right_on='player',
        how='left'
    )
    df.rename(columns={'player_id': 'captain_id'}, inplace=True)
    df.drop(columns=['player'], inplace=True)
    
    if 'team_id' in df.columns:
        df['team_id'] = df['team_id'].astype(str).str.replace('Q', '', regex=False)
        df['team_id'] = pd.to_numeric(df['team_id'], errors='coerce').astype('Int64')
    if 'opponent_id' in df.columns:
        df['opponent_id'] = df['opponent_id'].astype(str).str.replace('Q', '', regex=False)
        df['opponent_id'] = pd.to_numeric(df['opponent_id'], errors='coerce').astype('Int64')
    
    # CHUẨN HÓA CỘT round
    df["round"] = df["round"].apply(lambda x: x.split()[-1].zfill(2))
    
    # TẠO SUBSET CỘT FACT CUỐI
    df_subset = df[[
        'season',
        'game_id',
        'team_id',
        'opponent_id',
        'round',
        'venue',
        'result',
        'GF',
        'GA',
        'xG',
        'xGA',
        'Poss',
        'captain_id',
        'Formation',
        'Opp Formation',
    ]]
    
    output_path = os.path.join(DATA_PROCESSED_DIR, 'fact_team_match_clean.csv')
    df_subset.to_csv(output_path, index=False)
    print(f"fact_team_match_clean created: {len(df_subset)} records")
    return df_subset


def create_fact_player_match():
    print("Creating fact_player_match_clean:")
    
    df = pd.read_csv(
        os.path.join(DATA_DIR, "fbref_fact_player_match_stats.csv"),
        header=[0, 1, 2]
    )
    
    df_match = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_match.csv'))
    df_player = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_player.csv'))
    df_team = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_team.csv'))
    
    season_col_name = [col for col in df.columns if col[0] == 'season' and col[1] == 'Unnamed: 1_level_1'][0]
    game_col_name = [col for col in df.columns if col[0] == 'game' and col[1] == 'Unnamed: 2_level_1'][0]
    team_col_name = [col for col in df.columns if col[0] == 'team' and col[1] == 'Unnamed: 3_level_1'][0]
    player_col_name = [col for col in df.columns if col[0] == 'player' and col[1] == 'Unnamed: 4_level_1'][0]
    
    # Loại bỏ dòng đầu tiên nếu có giá trị "season" (dòng header thật trong file CSV)
    if len(df) > 0 and str(df.iloc[0][season_col_name]).lower() == 'season':
        df = df.iloc[1:].reset_index(drop=True)
        print(f"Đã loại bỏ dòng header trùng lặp. Số dòng còn lại: {len(df)}")
    
    min_col = [col for col in df.columns if col[0] == 'min' and col[1] == 'Unnamed: 9_level_1'][0]
    gls_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'Gls'][0]
    ast_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'Ast'][0]
    pk_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'PK'][0]
    pkatt_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'PKatt'][0]
    sh_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'Sh'][0]
    sot_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'SoT'][0]
    crdy_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'CrdY'][0]
    crdr_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'CrdR'][0]
    touches_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'Touches'][0]
    tkl_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'Tkl'][0]
    int_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'Int'][0]
    blocks_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'Blocks'][0]
    sca_col = [col for col in df.columns if col[0] == 'SCA' and col[1] == 'SCA'][0]
    gca_col = [col for col in df.columns if col[0] == 'SCA' and col[1] == 'GCA'][0]
    cmp_col = [col for col in df.columns if col[0] == 'Passes' and col[1] == 'Cmp'][0]
    att_col = [col for col in df.columns if col[0] == 'Passes' and col[1] == 'Att'][0]
    cmppct_col = [col for col in df.columns if col[0] == 'Passes' and col[1] == 'Cmp%'][0]
    prgp_col = [col for col in df.columns if col[0] == 'Passes' and col[1] == 'PrgP'][0]
    carries_col = [col for col in df.columns if col[0] == 'Carries' and col[1] == 'Carries'][0]
    prgc_col = [col for col in df.columns if col[0] == 'Carries' and col[1] == 'PrgC'][0]
    to_att_col = [col for col in df.columns if col[0] == 'Take-Ons' and col[1] == 'Att'][0]
    to_succ_col = [col for col in df.columns if col[0] == 'Take-Ons' and col[1] == 'Succ'][0]
    
    df_subset = df[[
        season_col_name,  # season
        game_col_name,  # game
        team_col_name,  # team
        player_col_name,  # player
        min_col,
        gls_col,
        ast_col,
        pk_col,
        pkatt_col,
        sh_col,
        sot_col,
        crdy_col,
        crdr_col,
        touches_col,
        tkl_col,
        int_col,
        blocks_col,
        sca_col,
        gca_col,
        cmp_col,
        att_col,
        cmppct_col,
        prgp_col,
        carries_col,
        prgc_col,
        to_att_col,
        to_succ_col
    ]].copy()
    
    df_subset.columns = [
        'season',
        'game',
        'team',
        'player',
        'min_played',
        'goals',
        'assists',
        'penalty_made',
        'penalty_attempted',
        'shots',
        'shots_on_target',
        'yellow_cards',
        'red_cards',
        'touches',
        'tackles',
        'interceptions',
        'blocks',
        'shot_creating_actions',
        'goal_creating_actions',
        'passes_completed',
        'passes_attempted',
        'pass_completion_percent',
        'progressive_passes',
        'carries',
        'progressive_carries',
        'take_ons_attempted',
        'take_ons_successful'
    ]
    
    # Tên khác form
    name_map = {
        "Brighton & Hove Albion": "Brighton",
        "Manchester United": "Manchester utd",
        "Newcastle United": "Newcastle utd",
        "Sheffield United": "Sheffield utd",
        "Tottenham Hotspur": "Tottenham",
        "West Bromwich Albion": "West brom",
        "West Ham United": "West ham",
        "Wolverhampton Wanderers": "Wolves",
        "Nottingham Forest": "Nott'ham forest"
    }
    df_subset["team"] = df_subset["team"].replace(name_map)
    
    # Chuẩn hóa chuỗi cho game
    df_subset['game'] = df_subset['game'].astype(str).str.strip().str.lower()
    df_match['game'] = df_match['game'].astype(str).str.strip().str.lower()
    
    # Map game → game_id
    df_subset = df_subset.merge(
        df_match[['game_id', 'game']],
        on='game',
        how='left'
    )
    
    # Chuẩn hóa cho team
    df_subset['team'] = df_subset['team'].astype(str).str.strip().str.lower()
    df_team['team_name'] = df_team['team_name'].astype(str).str.strip().str.lower()
    
    # Map team → team_id
    df_subset = df_subset.merge(
        df_team[['team_id', 'team_name']],
        left_on='team',
        right_on='team_name',
        how='left'
    )
    
    # Xóa cột thừa
    if 'team_name' in df_subset.columns:
        df_subset.drop(columns=['team_name'], inplace=True)
    
    # Đảm bảo team_id là integer (loại bỏ "Q" nếu có)
    if 'team_id' in df_subset.columns:
        df_subset['team_id'] = df_subset['team_id'].astype(str).str.replace('Q', '', regex=False)
        df_subset['team_id'] = pd.to_numeric(df_subset['team_id'], errors='coerce').astype('Int64')
    
    # Chuẩn hóa cho player
    df_subset['player'] = df_subset['player'].astype(str).str.strip().str.lower()
    df_player['player'] = df_player['player'].astype(str).str.strip().str.lower()
    
    # Map player → player_id
    df_subset = df_subset.merge(
        df_player[['player_id', 'player']],
        on='player',
        how='left'
    )
    
    
    df_subset = df_subset[[
        'season',
        'game_id',
        'team_id',
        'player_id',
        'min_played',
        'goals',
        'assists',
        'penalty_made',
        'penalty_attempted',
        'shots',
        'shots_on_target',
        'yellow_cards',
        'red_cards',
        'touches',
        'tackles',
        'interceptions',
        'blocks',
        'shot_creating_actions',
        'goal_creating_actions',
        'passes_completed',
        'passes_attempted',
        'pass_completion_percent',
        'progressive_passes',
        'carries',
        'progressive_carries',
        'take_ons_attempted',
        'take_ons_successful'
    ]]
    
    output_path = os.path.join(DATA_PROCESSED_DIR, 'fact_player_match_clean.csv')
    df_subset.to_csv(output_path, index=False)
    print(f"fact_player_match_clean created: {len(df_subset)} records")
    return df_subset


def create_fact_team_point():
    print("Creating fact_team_point:")
    
    df = pd.read_csv(os.path.join(DATA_DIR, 'premier_league_last_5_seasons.csv'))
    df_team = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_team.csv'))
    
    def convert_season(season):
        # season dạng "2024/2025"
        parts = season.split("/")
        y1 = parts[0][-2:]   # lấy 2 số cuối của năm đầu
        y2 = parts[1][-2:]   # lấy 2 số cuối của năm sau
        return y1 + y2       # ghép lại thành 2425
    
    df["Mùa giải"] = df["Mùa giải"].apply(convert_season)
    df = df.rename(columns={"Mùa giải": "season_id"})
    
    # Tên khác form
    name_map = {
        "Ipswich": "Ipswich Town",
        "Luton": "Luton Town",
        "Newcastle": "Newcastle utd",
        "Leeds": "Leeds United",
        "Leicester": "Leicester City",
        "Norwich": "Norwich City",
        "Nottingham": "Nott'ham forest"
    }
    df["Team"] = df["Team"].replace(name_map)
    
    # Chuẩn hóa cho team
    df['Team'] = df['Team'].astype(str).str.strip().str.lower()
    df_team['team_name'] = df_team['team_name'].astype(str).str.strip().str.lower()
    
    # Map team → team_id
    df = df.merge(
        df_team[['team_id', 'team_name']],
        left_on='Team',
        right_on='team_name',
        how='left'
    )
    
    # Xóa cột thừa
    if 'team_name' in df.columns:
        df.drop(columns=['team_name'], inplace=True)
    
    print("Team không map:", df[df['team_id'].isna()]['Team'].unique())
    
    # Đổi kiểu dữ liệu cột rank
    df["Rank"] = df["Rank"].astype(int)
    
    # Tách GF và GA từ cột "GF:GA"
    df[["GF", "GA"]] = df["GF:GA"].str.split(":", expand=True)
    
    # Chuyển kiểu dữ liệu sang int
    df["GF"] = df["GF"].astype(int)
    df["GA"] = df["GA"].astype(int)
    
    # Xóa cột cũ
    df.drop(columns=["GF:GA"], inplace=True)
    
    df_subset = df[[
        "season_id",
        "Match_Category",
        "Rank",
        "team_id",
        "MP",
        "W",
        "D",
        "L",
        "GF",
        "GA",
        "GD",
        "Pts",
        "Recent_Form"
    ]]
    
    output_path = os.path.join(DATA_PROCESSED_DIR, 'fact_team_point.csv')
    df_subset.to_csv(output_path, index=False)
    print(f"fact_team_point created: {len(df_subset)} records")
    return df_subset


if __name__ == "__main__":

    print("ETL Football - Transform Process")
 
    # Create dimension tables 
    create_dim_player()
    create_dim_team()
    create_dim_stadium()
    
    # Create fact tables (dua vao bang dimdim)
    create_fact_team_match()
    create_fact_player_match()
    create_fact_team_point()
    

    print("Transform process completed!")

