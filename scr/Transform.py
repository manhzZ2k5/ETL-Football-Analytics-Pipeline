import pandas as pd
import numpy as np
import os
from pathlib import Path

if "ETL_FOOTBALL_BASE_DIR" in os.environ:
    BASE_DIR = os.environ["ETL_FOOTBALL_BASE_DIR"]
else:
    BASE_DIR = str(Path(__file__).parent.parent.absolute())

DATA_DIR = os.path.join(BASE_DIR, "data_raw")
DATA_PROCESSED_DIR = os.path.join(BASE_DIR, "data_processed")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)



def save_table(df: pd.DataFrame, filename: str, table_type: str = "table") -> None:
    output_path = os.path.join(DATA_PROCESSED_DIR, filename)
    df.to_csv(output_path, index=False)
    print(f"{table_type} created: {len(df)} records -> {filename}")


def _get_column(df: pd.DataFrame, level0: str, level1: str | None = None, single_name: str | None = None):
    """Lấy đúng tên cột dù file có MultiIndex hay single header."""
    if isinstance(df.columns, pd.MultiIndex):
        candidates = []
        target_level0 = level0.lower()
        target_level1 = level1.lower() if level1 else None

        for col in df.columns:
            part0 = str(col[0]).strip()
            base, _, suffix = part0.partition('_')
            base_key = base.lower()
            suffix_key = suffix.lower() if suffix else None

            if base_key != target_level0:
                continue

            if target_level1:
                matches_level1 = []
                if len(col) > 1:
                    matches_level1.append(str(col[1]).strip().lower())
                if suffix_key:
                    matches_level1.append(suffix_key)

                if any(val == target_level1 for val in matches_level1 if val):
                    return col
                # nếu không khớp level1, bỏ qua cột này
                continue

            return col

        if not candidates and target_level1:
            raise KeyError(f"Column '{level0}_{level1}' not found in MultiIndex header")
        if not candidates:
            raise KeyError(f"Column '{level0}' not found in MultiIndex header")
        return candidates[0]

    col_map = {str(c).strip().lower(): c for c in df.columns}
    search_names = [single_name, level0]
    for name in search_names:
        if name and name.lower() in col_map:
            return col_map[name.lower()]
    raise KeyError(f"Column '{level0}' not found in header")


def create_dim_player() -> pd.DataFrame:

    print("Creating dim_player:")
    
    # SOURCE 1: Player SEASON stats (primary source with birth year)
    df_season = pd.read_csv(
        os.path.join(DATA_DIR, "fbref_fact_player_season_stats.csv"),
        header=0
    )
    
    player_col = _get_column(df_season, 'player', 'Unnamed: 3_level_1', 'player')
    pos_col = _get_column(df_season, 'pos', 'Unnamed: 5_level_1', 'pos')
    nation_col = _get_column(df_season, 'nation', 'Unnamed: 4_level_1', 'nation')
    born_col = _get_column(df_season, 'born', 'Unnamed: 7_level_1', 'born')
    
    df_season_subset = df_season[[player_col, pos_col, nation_col, born_col]].copy()
    df_season_subset.columns = ['player', 'pos', 'nation', 'born']
    
    # SOURCE 2: Player MATCH stats (to catch players missing in season stats)
    df_match = pd.read_csv(
        os.path.join(DATA_DIR, "fbref_fact_player_match_stats.csv"),
        header=0
    )
    
    # Remove header row if exists
    if len(df_match) > 0 and str(df_match.iloc[0, 0]).lower() == 'season':
        df_match = df_match.iloc[1:].reset_index(drop=True)
    
    # Extract player info from match stats
    player_col_match = _get_column(df_match, 'player', 'Unnamed: 4_level_1', 'player')
    pos_col_match = _get_column(df_match, 'pos', 'Unnamed: 7_level_1', 'pos')
    nation_col_match = _get_column(df_match, 'nation', 'Unnamed: 6_level_1', 'nation')
    
    df_match_subset = df_match[[player_col_match, pos_col_match, nation_col_match]].copy()
    df_match_subset.columns = ['player', 'pos', 'nation']
    df_match_subset['born'] = pd.NA  # Match stats don't have birth year
    
    # COMBINE both sources
    df_combined = pd.concat([df_season_subset, df_match_subset], ignore_index=True)
    
    # Remove duplicates - keep first occurrence (from season stats when available)
    df_combined = df_combined.drop_duplicates(subset='player', keep='first')
    
    # Remove rows with NaN player name
    df_combined = df_combined[df_combined['player'].notna()]
    
    # Sort by player name for consistent ordering
    df_combined = df_combined.sort_values('player').reset_index(drop=True)
    
    # Create player_id sequentially from 1
    df_combined['player_id'] = np.arange(len(df_combined)) + 1
    
    # Convert born to Int64
    df_combined['born'] = pd.to_numeric(df_combined['born'], errors='coerce').astype('Int64')
    
    # Reorder columns with player_id first
    df_combined = df_combined[['player_id', 'player', 'pos', 'nation', 'born']]
    
    # Print summary
    print(f"  -> {len(df_season_subset)} players from season stats")
    print(f"  -> {len(df_match_subset)} players from match stats")
    print(f"  -> {len(df_combined)} unique players total (after deduplication)")
    
    save_table(df_combined, "dim_player.csv", "dim_player")
    return df_combined


def create_dim_team() -> pd.DataFrame:
    print("Creating dim_team:")
    
    # dữ liệu dim_team
    df = pd.read_csv(os.path.join(DATA_DIR, "dim_team.csv"))

    # Loại bỏ dòng header trùng lặp (nếu có)
    header_row = list(df.columns)
    df = df[~df.apply(lambda row: list(row.values) == header_row, axis=1)].reset_index(drop=True)
    
    def pick_column(possible_names: list[str]) -> str:
        columns_lower = {str(col).strip().lower(): col for col in df.columns}
        for name in possible_names:
            if name in df.columns:
                return name
            lowered = name.lower()
            if lowered in columns_lower:
                return columns_lower[lowered]
        raise KeyError(f"Columns {possible_names} not found in dim_team source")

    column_map = {
        "team_id": ["club_id", "team_id"],
        "team_name": ["club_label", "team_name"],
        "founded_year": ["founding_year", "founded_year"],
        "stadium_id": ["venue_id", "stadium_id"],
    }

    resolved_columns = {key: pick_column(candidates) for key, candidates in column_map.items()}

    df_subset = df[
        [
            resolved_columns["team_id"],
            resolved_columns["team_name"],
            resolved_columns["founded_year"],
            resolved_columns["stadium_id"],
        ]
    ].copy()
    df_subset.columns = ['team_id', 'team_name', 'founded_year', 'stadium_id']
    
    # Map short names
    name_map_short = {
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
    
    if "short_name" in df.columns:
        df_subset["short_name"] = df["short_name"].copy()
    else:
        df_subset["short_name"] = pd.NA

    mask_missing_short = df_subset["short_name"].isna() | (df_subset["short_name"].astype(str).str.strip() == "")
    if mask_missing_short.any():
        df_subset.loc[mask_missing_short, "short_name"] = (
            df_subset.loc[mask_missing_short, "team_name"].replace(name_map_short)
        )
    
    # Clean team names - remove F.C., A.F.C., etc.
    remove_words = ["F.C.", "F.C", "FC", "AFC", "A.F.C.", "A.F.C"]
    
    def clean_team_name(name):
        for w in remove_words:
            name = name.replace(w, "")
        return name.strip()
    
    df_subset["team_name"] = df_subset["team_name"].apply(clean_team_name)
    
    # Normalize team names
    name_map = {
        "Brighton & Hove Albion": "Brighton",
        "Manchester United": "Manchester Utd",
        "Newcastle United": "Newcastle Utd",
        "Sheffield United": "Sheffield Utd",
        "Tottenham Hotspur": "Tottenham",
        "West Bromwich Albion": "West Brom",
        "West Ham United": "West Ham",
        "Wolverhampton Wanderers": "Wolves",
        "A Bournemouth": "Bournemouth",
        "Nottingham Forest": "Nott'Ham Forest"
    }
    df_subset["team_name"] = df_subset["team_name"].replace(name_map)
    
    # Loại bỏ "Q" và chuyển sang integer
    df_subset["team_id"] = df_subset["team_id"].astype(str).str.replace("Q", "", regex=False)
    df_subset["team_id"] = pd.to_numeric(df_subset["team_id"], errors='coerce').astype('Int64')
    
    df_subset["stadium_id"] = df_subset["stadium_id"].astype(str).str.replace("Q", "", regex=False)
    df_subset["stadium_id"] = pd.to_numeric(df_subset["stadium_id"], errors='coerce').astype('Int64')
    
    save_table(df_subset, "dim_team.csv", "dim_team")
    return df_subset


def create_dim_stadium() -> pd.DataFrame:
    print("Creating dim_stadium:")
    
    # dữ liệu Dim_stadium
    # Handle malformed lines by skipping them (on_bad_lines='skip' for pandas >= 1.3.0)
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, "dim_stadium.csv"), on_bad_lines='skip')
    except TypeError:
        # Fallback for older pandas versions
        try:
            df = pd.read_csv(os.path.join(DATA_DIR, "dim_stadium.csv"), error_bad_lines=False, warn_bad_lines=False)
        except TypeError:
            # If both fail, read with engine='python' which is more lenient
            df = pd.read_csv(os.path.join(DATA_DIR, "dim_stadium.csv"), engine='python', on_bad_lines='skip')
    
    # Filter out rows that don't have exactly 3 columns (malformed data)
    df = df[df.notna().sum(axis=1) == len(df.columns)].copy()
    
    # Loại bỏ dòng header trùng lặp (nếu có)
    header_row = list(df.columns)
    df = df[~df.apply(lambda row: list(row.values) == header_row, axis=1)].reset_index(drop=True)
    
    def pick_column(possible_names: list[str]) -> str:
        for name in possible_names:
            if name in df.columns:
                return name
            lowered = [c.lower() for c in df.columns]
            if name.lower() in lowered:
                return df.columns[lowered.index(name.lower())]
        raise KeyError(f"Columns {possible_names} not found in dim_stadium source")

    column_map = {
        "stadium_id": ["venue_id", "stadium_id"],
        "stadium_name": ["venue_label", "stadium_name", "statium_name"],
        "capacity": ["capacity"],
    }

    resolved_columns = {key: pick_column(candidates) for key, candidates in column_map.items()}

    df_subset = df[
        [
            resolved_columns["stadium_id"],
            resolved_columns["stadium_name"],
            resolved_columns["capacity"],
        ]
    ].copy()
    df_subset.columns = ["stadium_id", "statium_name", "capacity"]
    
    # Filter out rows where stadium_id, stadium_name, or capacity is NaN (malformed data)
    initial_count = len(df_subset)
    df_subset = df_subset.dropna(subset=['stadium_id', 'statium_name', 'capacity']).copy()
    if len(df_subset) < initial_count:
        print(f"  -> Removed {initial_count - len(df_subset)} malformed rows")
    
    # Loại bỏ dòng có giá trị "capacity" trong cột capacity (nếu còn)
    df_subset = df_subset[df_subset['capacity'].astype(str).str.lower() != 'capacity'].reset_index(drop=True)
    
    # Loại bỏ "Q" và chuyển stadium_id sang integer
    df_subset['stadium_id'] = df_subset['stadium_id'].astype(str).str.replace('Q', '', regex=False)
    df_subset['stadium_id'] = pd.to_numeric(df_subset['stadium_id'], errors='coerce').astype('Int64')
    
    # Chuyển capacity sang integer
    df_subset['capacity'] = pd.to_numeric(df_subset['capacity'], errors='coerce')
    df_subset = df_subset.dropna(subset=['capacity'])
    df_subset['capacity'] = df_subset['capacity'].astype(int)
    
    save_table(df_subset, "dim_stadium.csv", "dim_stadium")
    return df_subset


def create_dim_match() -> pd.DataFrame:
    print("Creating dim_match:")
    
    df_raw = pd.read_csv(os.path.join(DATA_DIR, "fbref_fact_team_match.csv"))
    
    # remove duplicates
    unique_matches = df_raw.drop_duplicates(subset=['game']).copy()
    print(f"  -> After removing duplicates: {len(unique_matches)} unique games")
    
    # Create index
    unique_matches = unique_matches.reset_index(drop=True)
    unique_matches['game_id'] = range(1, len(unique_matches) + 1)
    
    # convert data types
    df_subset = unique_matches[['game_id', 'game', 'date', 'round', 'day']].copy()
    df_subset['game'] = df_subset['game'].astype(str).str.strip()
    
    # Clean date format 
    df_subset['date'] = df_subset['date'].astype(str).str.split(' ').str[0]
    df_subset['date'] = pd.to_datetime(df_subset['date'], errors='coerce')
    
    df_subset['round'] = df_subset['round'].astype(str).str.strip()
    df_subset['day'] = df_subset['day'].astype(str).str.strip()
    
    # remove missing date after datetime conversion
    before_count = len(df_subset)
    df_subset = df_subset.dropna(subset=['date']).copy()
    removed_count = before_count - len(df_subset)
    print(f"  -> Removed {removed_count} rows with invalid/missing dates")
    
    # Recreate game_id for remaining rows
    df_subset = df_subset.reset_index(drop=True)
    df_subset['game_id'] = range(1, len(df_subset) + 1)
    df_subset['game_id'] = df_subset['game_id'].astype('Int64')
    
    print(f"  -> Final: {len(df_subset)} games with valid dates")
    
    save_table(df_subset, "dim_match.csv", "dim_match")
    return df_subset



def create_fact_team_match() -> pd.DataFrame:
    """Create fact table for team match statistics."""
    print("Creating fact_team_match_clean:")
    
    df = pd.read_csv(os.path.join(DATA_DIR, "fbref_fact_team_match.csv"))
    # Chỉ dropna cho các cột bắt buộc, giữ lại các trận chưa đá (có thể có NaN ở GF, GA, etc.)
    df = df.dropna(subset=['team', 'opponent', 'game'])
    
    df_team = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_team.csv'))
    df_match = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_match.csv'))
    df_player = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_player.csv'))
    
    # CHUẨN HÓA CHUỖI (rất quan trọng)
    df['team'] = df['team'].astype(str).str.strip().str.lower()
    df_team['team_name'] = df_team['team_name'].astype(str).str.strip().str.lower()
    
    df['game'] = df['game'].astype(str).str.strip().str.lower()
    df_match['game'] = df_match['game'].astype(str).str.strip().str.lower()
    
    df['Captain'] = df['Captain'].astype(str).str.strip().str.lower()
    df_player['player'] = df_player['player'].astype(str).str.strip().str.lower()
    
    df['opponent'] = df['opponent'].astype(str).str.strip().str.lower()
    
    # Normalize team names to match dim_team (before merge)
    name_map = {
        "brighton & hove albion": "brighton",
        "manchester united": "manchester utd",
        "newcastle united": "newcastle utd",
        "sheffield united": "sheffield utd",
        "tottenham hotspur": "tottenham",
        "west bromwich albion": "west brom",
        "west ham united": "west ham",
        "wolverhampton wanderers": "wolves",
        "nottingham forest": "nott'ham forest",
        "sunderland a.": "sunderland",  # Map "Sunderland A." variants to "Sunderland" to match dim_team
        "sunderland a f c": "sunderland",
        "swansea city a.": "swansea city a.",  # Keep as is - matches dim_team
        "hull city a.": "hull city a."  # Keep as is - matches dim_team
    }
    df['team'] = df['team'].replace(name_map)
    df['opponent'] = df['opponent'].replace(name_map)
    
    # Apply clean function giống như trong create_dim_team
    remove_words = ["f.c.", "f.c", "fc", "afc", "a.f.c.", "a.f.c"]
    def clean_team_name(name):
        # name đã lowercase rồi, chỉ cần remove words
        for w in remove_words:
            name = name.replace(w, "")
        # Remove standalone "a." at the end (e.g., "sunderland a." -> "sunderland")
        name = name.rstrip(" .").replace(" a.", "").replace(" a ", " ").strip()
        return name.strip()
    
    df['team'] = df['team'].apply(clean_team_name)
    df['opponent'] = df['opponent'].apply(clean_team_name)

    # Map Captain → captain_id
    df = df.merge(
        df_player[['player_id', 'player']],
        left_on='Captain',
        right_on='player',
        how='left'
    )
    df.rename(columns={'player_id': 'captain_id'}, inplace=True)
    df.drop(columns=['player'], inplace=True)
    
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
    

    # Loại bỏ "Q" và chuyển team_id và opponent_id sang integer
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
    df_subset = df_subset.dropna(subset=['result'])
    save_table(df_subset, 'fact_team_match_clean.csv', "fact_team_match_clean")
    return df_subset


def create_fact_player_match() -> pd.DataFrame:
    print("Creating fact_player_match_clean:")
    
    # dữ liệu fact_playermatchstats
    df = pd.read_csv(
        os.path.join(DATA_DIR, "fbref_fact_player_match_stats.csv"),
        header=0
    )
    
    df_match = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_match.csv'))
    df_player = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_player.csv'))
    df_team = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_team.csv'))
    
    # Xác định tên cột linh hoạt cho cả MultiIndex và single-level
    season_col_name = _get_column(df, 'season', 'Unnamed: 1_level_1', 'season')
    game_col_name = _get_column(df, 'game', 'Unnamed: 2_level_1', 'game')
    team_col_name = _get_column(df, 'team', 'Unnamed: 3_level_1', 'team')
    player_col_name = _get_column(df, 'player', 'Unnamed: 4_level_1', 'player')
    
    # Loại bỏ dòng đầu tiên nếu có giá trị "season" (dòng header thật trong file CSV)
    if len(df) > 0 and str(df.iloc[0][season_col_name]).lower() == 'season':
        df = df.iloc[1:].reset_index(drop=True)
        print(f"Đã loại bỏ dòng header trùng lặp. Số dòng còn lại: {len(df)}")
    
    # Tìm các cột chỉ số (MultiIndex hoặc single)
    min_col = _get_column(df, 'min', 'Unnamed: 9_level_1', 'min')
    gls_col = _get_column(df, 'Performance', 'Gls', 'Performance_Gls')
    ast_col = _get_column(df, 'Performance', 'Ast', 'Performance_Ast')
    pk_col = _get_column(df, 'Performance', 'PK', 'Performance_PK')
    pkatt_col = _get_column(df, 'Performance', 'PKatt', 'Performance_PKatt')
    sh_col = _get_column(df, 'Performance', 'Sh', 'Performance_Sh')
    sot_col = _get_column(df, 'Performance', 'SoT', 'Performance_SoT')
    crdy_col = _get_column(df, 'Performance', 'CrdY', 'Performance_CrdY')
    crdr_col = _get_column(df, 'Performance', 'CrdR', 'Performance_CrdR')
    touches_col = _get_column(df, 'Performance', 'Touches', 'Performance_Touches')
    tkl_col = _get_column(df, 'Performance', 'Tkl', 'Performance_Tkl')
    int_col = _get_column(df, 'Performance', 'Int', 'Performance_Int')
    blocks_col = _get_column(df, 'Performance', 'Blocks', 'Performance_Blocks')
    xg_col = _get_column(df, 'Expected', 'xG', 'Expected_xG')
    xag_col = _get_column(df, 'Expected', 'xAG', 'Expected_xAG')
    sca_col = _get_column(df, 'SCA', 'SCA', 'SCA_SCA')
    gca_col = _get_column(df, 'SCA', 'GCA', 'SCA_GCA')
    cmp_col = _get_column(df, 'Passes', 'Cmp', 'Passes_Cmp')
    att_col = _get_column(df, 'Passes', 'Att', 'Passes_Att')
    cmppct_col = _get_column(df, 'Passes', 'Cmp%', 'Passes_Cmp%')
    prgp_col = _get_column(df, 'Passes', 'PrgP', 'Passes_PrgP')
    carries_col = _get_column(df, 'Carries', 'Carries', 'Carries_Carries')
    prgc_col = _get_column(df, 'Carries', 'PrgC', 'Carries_PrgC')
    to_att_col = _get_column(df, 'Take-Ons', 'Att', 'Take-Ons_Att')
    to_succ_col = _get_column(df, 'Take-Ons', 'Succ', 'Take-Ons_Succ')
    
    df_subset = df[[season_col_name, game_col_name, team_col_name, player_col_name,
                    min_col, gls_col, xg_col, xag_col, ast_col, pk_col, pkatt_col, sh_col, sot_col,
                    crdy_col, crdr_col, touches_col, tkl_col, int_col, blocks_col,
                    sca_col, gca_col, cmp_col, att_col, cmppct_col, prgp_col,
                    carries_col, prgc_col, to_att_col, to_succ_col]].copy()
    
    df_subset.columns = [
        'season', 'game', 'team', 'player', 'min_played', 'goals', 'xG', 'xA',
        'assists', 'penalty_made', 'penalty_attempted', 'shots', 'shots_on_target',
        'yellow_cards', 'red_cards', 'touches', 'tackles', 'interceptions',
        'blocks', 'shot_creating_actions', 'goal_creating_actions',
        'passes_completed', 'passes_attempted', 'pass_completion_percent',
        'progressive_passes', 'carries', 'progressive_carries',
        'take_ons_attempted', 'take_ons_successful'
    ]
    
    # Normalize team names to match dim_team (before merge)
    name_map = {
        "Brighton & Hove Albion": "Brighton",
        "Manchester United": "Manchester utd",
        "Newcastle United": "Newcastle utd",
        "Sheffield United": "Sheffield utd",
        "Tottenham Hotspur": "Tottenham",
        "West Bromwich Albion": "West brom",
        "West Ham United": "West ham",
        "Wolverhampton Wanderers": "Wolves",
        "Nottingham Forest": "Nott'ham forest",
        "Sunderland A.": "Sunderland",  # Map "Sunderland A." variants to "Sunderland" to match dim_team
        "Sunderland A F C": "Sunderland",
        "Swansea City A.": "Swansea City A.",  # Keep as is - matches dim_team
        "Hull City A.": "Hull City A."  # Keep as is - matches dim_team
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
    
    # Additional mapping after lowercasing to catch any remaining variants
    name_map_lower = {
        "sunderland a.": "sunderland",
        "sunderland a f c": "sunderland",
    }
    df_subset['team'] = df_subset['team'].replace(name_map_lower)
    
    # Apply clean function giống như trong create_dim_team
    remove_words = ["f.c.", "f.c", "fc", "afc", "a.f.c.", "a.f.c"]
    def clean_team_name(name):
        name_lower = name.lower()
        for w in remove_words:
            name_lower = name_lower.replace(w, "")
        # Remove standalone "a." at the end (e.g., "sunderland a." -> "sunderland")
        name_lower = name_lower.rstrip(" .").replace(" a.", "").replace(" a ", " ").strip()
        return name_lower.strip()
    
    df_subset['team'] = df_subset['team'].apply(clean_team_name)
    
    # Map team → team_id
    df_subset = df_subset.merge(
        df_team[['team_id', 'team_name']],
        left_on='team',
        right_on='team_name',
        how='left'
    )
    
    # Filter out rows with NULL team_id (unmatched team names)
    initial_count = len(df_subset)
    null_team = df_subset[df_subset['team_id'].isna()]
    
    if len(null_team) > 0:
        unmatched_teams = null_team['team'].unique()
        print(f"  Warning: {len(null_team)} rows with unmatched team names: {list(unmatched_teams)[:5]}")
    
    df_subset = df_subset.dropna(subset=['team_id'])
    filtered_count = initial_count - len(df_subset)
    if filtered_count > 0:
        print(f"  -> Filtered out {filtered_count} rows with NULL team_id")
    
    # Xóa cột thừa
    if 'team_name' in df_subset.columns:
        df_subset.drop(columns=['team_name'], inplace=True)
    
    # Đảm bảo team_id là integer (loại bỏ "Q" nếu có)
    if 'team_id' in df_subset.columns:
        df_subset['team_id'] = df_subset['team_id'].astype(str).str.replace('Q', '', regex=False)
        df_subset['team_id'] = pd.to_numeric(df_subset['team_id'], errors='coerce').astype('Int64')
    
    # Filter again after conversion
    df_subset = df_subset.dropna(subset=['team_id'])
    
    # Chuẩn hóa cho player
    df_subset['player'] = df_subset['player'].astype(str).str.strip().str.lower()
    df_player['player'] = df_player['player'].astype(str).str.strip().str.lower()
    
    # Map player → player_id
    df_subset = df_subset.merge(
        df_player[['player_id', 'player']],
        on='player',
        how='left'
    )
    
    # Filter out rows with NULL player_id or game_id
    initial_count = len(df_subset)
    null_player = df_subset[df_subset['player_id'].isna()]
    null_game = df_subset[df_subset['game_id'].isna()]
    
    if len(null_player) > 0:
        print(f"  Warning: {len(null_player)} rows with unmatched player names")
    if len(null_game) > 0:
        print(f"  Warning: {len(null_game)} rows with unmatched game names")
    
    df_subset = df_subset.dropna(subset=['player_id', 'game_id'])
    filtered_count = initial_count - len(df_subset)
    if filtered_count > 0:
        print(f"  -> Filtered out {filtered_count} rows with NULL player_id or game_id")
    
    df_subset = df_subset[['season', 'game_id', 'team_id', 'player_id',
                           'min_played', 'goals', 'xG', 'xA', 'assists', 'penalty_made',
                           'penalty_attempted', 'shots', 'shots_on_target',
                           'yellow_cards', 'red_cards', 'touches', 'tackles',
                           'interceptions', 'blocks', 'shot_creating_actions',
                           'goal_creating_actions', 'passes_completed',
                           'passes_attempted', 'pass_completion_percent',
                           'progressive_passes', 'carries', 'progressive_carries',
                           'take_ons_attempted', 'take_ons_successful']]
    
    save_table(df_subset, 'fact_player_match_clean.csv', "fact_player_match_clean")
    return df_subset


def create_fact_team_point() -> pd.DataFrame:
    print("Creating fact_team_point:")
    
    if os.path.exists(os.path.join(DATA_DIR, 'team_point.csv')):
        df = pd.read_csv(os.path.join(DATA_DIR, 'team_point.csv'))
    else:
        print("  -> team_point.csv not found, skipping")
        return pd.DataFrame()
    
    df_team = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_team.csv'))
    
    # --- Logic convert season giữ nguyên ---
    def convert_season(season):
        season_str = str(season).strip().replace("/", "-")
        if "-" in season_str:
            parts = season_str.split("-")
            if len(parts) == 2:
                y1 = parts[0][-2:]
                y2 = parts[1][-2:]
                return int(y1 + y2)
        return season

    if "Mùa giải" in df.columns:
        df["Mùa giải"] = df["Mùa giải"].apply(convert_season)
        df = df.rename(columns={"Mùa giải": "season_id"})
    elif "season_id" in df.columns:
        df["season_id"] = df["season_id"].apply(convert_season)
    
    # --- Mapping tên đội ---
    name_map = {
        "Ipswich": "Ipswich Town",
        "Luton": "Luton Town",
        "Newcastle": "Newcastle utd",
        "Leeds": "Leeds United",
        "Leicester": "Leicester City",
        "Norwich": "Norwich City",
        "Nottingham": "Nott'ham forest",
        "Sunderland A.": "Sunderland",
        "Sunderland A F C": "Sunderland",
        "Swansea City A.": "Swansea City A.",
        "Hull City A.": "Hull City A."
    }
    df["Team"] = df["Team"].replace(name_map)
    
    # Chuẩn hóa text
    df['Team'] = df['Team'].astype(str).str.strip().str.lower()
    df_team['team_name'] = df_team['team_name'].astype(str).str.strip().str.lower()
    
    name_map_lower = {
        "sunderland a.": "sunderland",
        "sunderland a f c": "sunderland",
    }
    df['Team'] = df['Team'].replace(name_map_lower)
    
    remove_words = ["f.c.", "f.c", "fc", "afc", "a.f.c.", "a.f.c"]
    def clean_team_name(name):
        name_lower = name.lower()
        for w in remove_words:
            name_lower = name_lower.replace(w, "")
        name_lower = name_lower.rstrip(" .").replace(" a.", "").replace(" a ", " ").strip()
        return name_lower.strip()
    
    df['Team'] = df['Team'].apply(clean_team_name)
    
    # Merge lấy team_id
    df = df.merge(
        df_team[['team_id', 'team_name']],
        left_on='Team',
        right_on='team_name',
        how='left'
    )
    
    df = df.dropna(subset=['team_id'])
    
    if 'team_name' in df.columns:
        df.drop(columns=['team_name'], inplace=True)
    
    # --- SỬA LỖI RANK TẠI ĐÂY ---
    # Logic: Chuyển sang string -> Tách bằng dấu chấm -> Lấy phần đầu tiên -> Chuyển sang int
    # Ví dụ: "1." -> "1" | "1.0" -> "1" | "10." -> "10"
    try:
        df["Rank"] = df["Rank"].astype(str).str.split('.').str[0].astype(int)
    except ValueError as e:
        print(f"  Error converting Rank: {e}")
        # Fallback: Nếu lỗi, ép về 0 hoặc giữ nguyên để debug (ở đây ta chọn xóa dòng lỗi)
        df = df[pd.to_numeric(df["Rank"], errors='coerce').notnull()]
        df["Rank"] = df["Rank"].astype(int)
    # -----------------------------
    
    df[["GF", "GA"]] = df["GF:GA"].str.split(":", expand=True)
    df["GF"] = df["GF"].astype(int)
    df["GA"] = df["GA"].astype(int)
    
    df.drop(columns=["GF:GA"], inplace=True)
    
    df_subset = df[["season_id", "Match_Category", "Rank", "team_id",
                    "MP", "W", "D", "L", "GF", "GA", "GD", "Pts", "Recent_Form"]]
    
    save_table(df_subset, 'fact_team_point.csv', "fact_team_point")
    return df_subset

if __name__ == "__main__":
    print("ETL Football - Transform Process")

    # Create dimension tables (must be ready before fact tables)
    create_dim_player()
    create_dim_team()
    create_dim_stadium()
    create_dim_match()

    # Create fact tables (depend on dimension tables)
    create_fact_team_match()
    create_fact_player_match()
    create_fact_team_point()

    print("Transform process completed!")