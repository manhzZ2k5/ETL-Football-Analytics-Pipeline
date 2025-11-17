from configparser import ConfigParser
import psycopg2
import os
from pathlib import Path
import pandas as pd
from io import StringIO

if "ETL_FOOTBALL_BASE_DIR" in os.environ:
    BASE_DIR = os.environ["ETL_FOOTBALL_BASE_DIR"]
else:
    BASE_DIR = str(Path(__file__).parent.parent.absolute())

DATA_PROCESSED_DIR = os.path.join(BASE_DIR, "data_processed")
os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)


def truncate_table(cursor, table_name, cascade=False):
    """
    Remove all records in the table before inserting fresh data.
    Set cascade=True for tables referenced by foreign keys.
    """
    cascade_clause = " CASCADE" if cascade else ""
    cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY{cascade_clause};")


def load_config(filename='database.ini', section='postgresql'):
    current_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    file_path = os.path.join(current_dir, filename)

    if not os.path.exists(file_path):
        raise Exception(f"File {file_path} not found")

    parser = ConfigParser()
    parser.read(file_path)

    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        sections = parser.sections()
        raise Exception(f"Section {section} not found in {file_path}. Available sections: {sections}")

    # Docker environment: Replace localhost with host.docker.internal
    if 'host' in config and config['host'] == 'localhost':
        # Check if running in Docker
        if os.path.exists('/.dockerenv') or os.environ.get('AIRFLOW_HOME'):
            config['host'] = 'host.docker.internal'
            print(f"[Docker detected] Using host.docker.internal for PostgreSQL connection")

    return config

def connect(config):
    conn = None
    try:
        conn = psycopg2.connect(**config)
        print("Connected")
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error : {error}")
        return None

def load_dim_stadium(cursor, csv_path):
    sql1 = '''
    CREATE TABLE IF NOT EXISTS Dim_Stadium (
        stadium_id INT PRIMARY KEY,
        stadium_name VARCHAR(255) NOT NULL,
        capacity INT
    );
    '''
    cursor.execute(sql1)
    
    # Đọc CSV và rename column
    df = pd.read_csv(csv_path)
    df_renamed = df.rename(columns={'statium_name': 'stadium_name'})
    
    # UPSERT từng record (ON CONFLICT DO UPDATE)
    upsert_sql = """
        INSERT INTO Dim_Stadium (stadium_id, stadium_name, capacity)
        VALUES (%s, %s, %s)
        ON CONFLICT (stadium_id) 
        DO UPDATE SET 
            stadium_name = EXCLUDED.stadium_name,
            capacity = EXCLUDED.capacity;
    """
    
    records = df_renamed.values.tolist()
    cursor.executemany(upsert_sql, records)
    print(f"dim_stadium upserted: {len(records)} records")


def load_dim_team(cursor, csv_path):
    """Load dim_team với UPSERT - cập nhật hoặc insert dữ liệu mới"""
    sql1 = '''
    CREATE TABLE IF NOT EXISTS Dim_Team (
        team_id INT PRIMARY KEY,
        team_name VARCHAR(255) NOT NULL,
        founded_year INT,
        stadium_id INT,
        short_name VARCHAR(20)
    );
    '''
    cursor.execute(sql1)
    
    df = pd.read_csv(csv_path)
    
    # UPSERT từng record
    upsert_sql = """
        INSERT INTO Dim_Team (team_id, team_name, founded_year, stadium_id, short_name)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (team_id)
        DO UPDATE SET
            team_name = EXCLUDED.team_name,
            founded_year = EXCLUDED.founded_year,
            stadium_id = EXCLUDED.stadium_id,
            short_name = EXCLUDED.short_name;
    """
    
    records = df.values.tolist()
    cursor.executemany(upsert_sql, records)
    print(f"dim_team upserted: {len(records)} records")


def load_dim_match(cursor, csv_path):
    sql1 = '''
    CREATE TABLE IF NOT EXISTS Dim_Match (
        match_id INT PRIMARY KEY,
        match_name VARCHAR(255) NOT NULL,
        match_date DATE
    );
    '''
    cursor.execute(sql1)
    
    df = pd.read_csv(csv_path)
    df_selected = df[['game_id', 'game', 'date']].copy()
    df_selected.rename(columns={'date': 'game_date', 'game': 'match_name', 'game_id': 'match_id'}, inplace=True)
    
    # Convert NaN to None (NULL in SQL)
    df_selected = df_selected.replace({pd.NA: None, pd.NaT: None})
    df_selected = df_selected.where(pd.notna(df_selected), None)
    
    upsert_sql = """
        INSERT INTO Dim_Match (match_id, match_name, match_date)
        VALUES (%s, %s, %s)
        ON CONFLICT (match_id)
        DO UPDATE SET
            match_name = EXCLUDED.match_name,
            match_date = EXCLUDED.match_date;
    """
    
    records = df_selected.values.tolist()
    cursor.executemany(upsert_sql, records)
    print(f"dim_match upserted: {len(records)} records")


def load_dim_player(cursor, csv_path):
    sql1 = '''
    CREATE TABLE IF NOT EXISTS Dim_Player (
        player_id INT PRIMARY KEY,
        player_name VARCHAR(100) NOT NULL,
        pos VARCHAR(20),
        nation VARCHAR(20),
        born INT
    );
    ''' 
    cursor.execute(sql1)
    
    df = pd.read_csv(csv_path)
    df_renamed = df.rename(columns={'player': 'player_name'})
    
    # Chuyển đổi cột 'born' từ float sang integer
    if 'born' in df_renamed.columns:
        df_renamed['born'] = pd.to_numeric(df_renamed['born'], errors='coerce').astype('Int64')
    
    upsert_sql = """
        INSERT INTO Dim_Player (player_id, player_name, pos, nation, born)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (player_id)
        DO UPDATE SET
            player_name = EXCLUDED.player_name,
            pos = EXCLUDED.pos,
            nation = EXCLUDED.nation,
            born = EXCLUDED.born;
    """
    
    df_renamed = df_renamed.replace({pd.NA: None, pd.NaT: None})
    records = df_renamed.values.tolist()
    cursor.executemany(upsert_sql, records)
    print(f"dim_player upserted: {len(records)} records")

def load_dim_season(cursor, csv_path):
    sql1 = '''
    CREATE TABLE IF NOT EXISTS Dim_season (
        season_id INT PRIMARY KEY,
        season_name VARCHAR(15) UNIQUE,
        start_year INT,
        end_year INT,
        actual_start_date DATE,
        actual_end_date DATE
    );
    ''' 
    cursor.execute(sql1)
    
    df = pd.read_csv(csv_path)
    
    upsert_sql = """
        INSERT INTO Dim_season (season_id, season_name, start_year, end_year, actual_start_date, actual_end_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (season_id)
        DO UPDATE SET
            season_name = EXCLUDED.season_name,
            start_year = EXCLUDED.start_year,
            end_year = EXCLUDED.end_year,
            actual_start_date = EXCLUDED.actual_start_date,
            actual_end_date = EXCLUDED.actual_end_date;
    """
    
    import numpy as np
    df = df.replace({pd.NA: None, pd.NaT: None, np.nan: None})
    records = df.values.tolist()
    cursor.executemany(upsert_sql, records)
    print(f"dim_season upserted: {len(records)} records")

def fact_team_match(cursor, csv_path):
    """Load fact_team_match - chỉ insert dữ liệu mới (DO NOTHING nếu đã tồn tại)"""
    sql1 = '''
    CREATE TABLE IF NOT EXISTS fact_team_match (
    season          INT,
    game_id         INT,
    team_id         INT,
    opponent_id     INT,
    round           INT,
    venue           VARCHAR(10),
    result          CHAR(1),
    "GF"            INT,
    "GA"            INT,
    "xG"            NUMERIC(4,2),
    "xGA"           NUMERIC(4,2),
    "Poss"          INT,
    "Formation"     VARCHAR(20),
    "Opp Formation" VARCHAR(20),

    PRIMARY KEY (season, game_id, team_id),

    FOREIGN KEY (season) REFERENCES dim_season(season_id),
    FOREIGN KEY (game_id) REFERENCES dim_match(match_id),
    FOREIGN KEY (team_id) REFERENCES dim_team(team_id),
    FOREIGN KEY (opponent_id) REFERENCES dim_team(team_id)
);
    '''
    cursor.execute(sql1)
    
    # Kiểm tra file new data có tồn tại không
    if not os.path.exists(csv_path):
        print(f"No new data")
        return
    
    df = pd.read_csv(csv_path)
    if len(df) == 0:
        print(f"No new records")
        return
    
    # Loại bỏ cột captain_id 
    if 'captain_id' in df.columns:
        df = df.drop(columns=['captain_id'])
    
    # Chỉ chọn các cột cần thiết theo đúng thứ tự trong SQL
    columns_to_insert = [
        'season', 'game_id', 'team_id', 'opponent_id', 'round', 'venue', 'result',
        'GF', 'GA', 'xG', 'xGA', 'Poss', 'Formation', 'Opp Formation'
    ]
    df = df[columns_to_insert]
    
    # INSERT chỉ những records chưa có (skip records đã tồn tại)
    insert_sql = """
        INSERT INTO fact_team_match (
            season, game_id, team_id, opponent_id, round, venue, result,
            "GF", "GA", "xG", "xGA", "Poss", "Formation", "Opp Formation"
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (season, game_id, team_id)
        DO NOTHING;
    """
    
    import numpy as np
    df = df.replace({pd.NA: None, pd.NaT: None, np.nan: None})
    records = df.values.tolist()
    
    # Đếm số records đã có trong database
    cursor.execute("SELECT COUNT(*) FROM fact_team_match")
    count_before = cursor.fetchone()[0]
    
    cursor.executemany(insert_sql, records)
    
    # Đếm sau khi insert
    cursor.execute("SELECT COUNT(*) FROM fact_team_match")
    count_after = cursor.fetchone()[0]
    
    inserted = count_after - count_before
    skipped = len(records) - inserted
    print(f"{inserted} new, {skipped} skipped, {count_after} total in DB")

def load_fact_team_point(cursor, csv_path):
    sql1 = '''
    CREATE TABLE IF NOT EXISTS Fact_Team_Point (
        season_id INT NOT NULL,
        "Match_Category" VARCHAR(100) NOT NULL,
        "Rank" INT NOT NULL,
        team_id INT NOT NULL,
        "MP" INT,
        "W" INT,
        "D" INT,
        "L" INT,
        "GF" INT,
        "GA" INT,
        "GD" INT,
        "Pts" INT,
        "Recent_Form" VARCHAR(50),
        PRIMARY KEY (season_id, team_id, "Match_Category"),
        FOREIGN KEY (season_id) REFERENCES dim_season(season_id),
        FOREIGN KEY (team_id) REFERENCES dim_team(team_id)
    );
    '''
    cursor.execute(sql1)
    
    if not os.path.exists(csv_path):
        print(f"No new data, skipping")
        return
    
    df = pd.read_csv(csv_path)
    if len(df) == 0:
        print(f"No new records, skipping")
        return
    
    # INSERT chỉ những records chưa có (skip records đã tồn tại)
    insert_sql = """
        INSERT INTO Fact_Team_Point (
            season_id, "Match_Category", "Rank", team_id,
            "MP", "W", "D", "L", "GF", "GA", "GD", "Pts", "Recent_Form"
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (season_id, team_id, "Match_Category")
        DO NOTHING;
    """
    
    import numpy as np
    df = df.replace({pd.NA: None, pd.NaT: None, np.nan: None})
    records = df.values.tolist()
    
    # Đếm số records đã có trong database
    cursor.execute("SELECT COUNT(*) FROM Fact_Team_Point")
    count_before = cursor.fetchone()[0]
    
    cursor.executemany(insert_sql, records)
    
    # Đếm sau khi insert
    cursor.execute("SELECT COUNT(*) FROM Fact_Team_Point")
    count_after = cursor.fetchone()[0]
    
    inserted = count_after - count_before
    skipped = len(records) - inserted
    print(f"{inserted} new, {skipped} skipped, {count_after} total in DB")

def fact_player_match(cursor, csv_path):
    sql1 = '''
    CREATE TABLE IF NOT EXISTS fact_player_match (
    season                  INT,
    game_id                 INT,
    team_id                 INT,
    player_id               INT,
    min_played              INT,
    goals                   INT,
    xG                      NUMERIC(4,2),
    xA                      NUMERIC(4,2),
    assists                 INT,
    penalty_made            INT,
    penalty_attempted       INT,
    shots                   INT,
    shots_on_target         INT,
    yellow_cards            INT,
    red_cards               INT,
    touches                 INT,
    tackles                 INT,
    interceptions           INT,
    blocks                  INT,
    shot_creating_actions   INT,
    goal_creating_actions   INT,
    passes_completed        INT,
    passes_attempted        INT,
    pass_completion_percent NUMERIC(5,2),
    progressive_passes      INT,
    carries                 INT,
    progressive_carries     INT,
    take_ons_attempted      INT,
    take_ons_successful     INT,

    PRIMARY KEY (season, game_id, team_id, player_id),

    FOREIGN KEY (season)      REFERENCES dim_season(season_id),
    FOREIGN KEY (game_id)     REFERENCES dim_match(match_id),
    FOREIGN KEY (team_id)     REFERENCES dim_team(team_id),
    FOREIGN KEY (player_id)   REFERENCES dim_player(player_id)
);
    '''
    cursor.execute(sql1)
    
    if not os.path.exists(csv_path):
        print(f"No new data, skipping")
        return
    
    df = pd.read_csv(csv_path)
    if len(df) == 0:
        print(f"No new records, skipping")
        return
    
    # INSERT chỉ những records chưa có (skip records đã tồn tại)
    insert_sql = """
        INSERT INTO fact_player_match (
            season, game_id, team_id, player_id, min_played, goals, xG, xA, assists,
            penalty_made, penalty_attempted, shots, shots_on_target, yellow_cards, red_cards,
            touches, tackles, interceptions, blocks, shot_creating_actions, goal_creating_actions,
            passes_completed, passes_attempted, pass_completion_percent, progressive_passes,
            carries, progressive_carries, take_ons_attempted, take_ons_successful
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (season, game_id, team_id, player_id)
        DO NOTHING;
    """
    
    import numpy as np
    df = df.replace({pd.NA: None, pd.NaT: None, np.nan: None})
    
    # Filter out rows with NULL player_id (cannot insert - part of PRIMARY KEY)
    initial_count = len(df)
    df = df[df['player_id'].notna()]
    filtered_count = initial_count - len(df)
    if filtered_count > 0:
        print(f"Filtered out {filtered_count} rows with NULL player_id (cannot be in PRIMARY KEY)")
    
    records = df.values.tolist()
    
    # Đếm số records đã có trong database
    cursor.execute("SELECT COUNT(*) FROM fact_player_match")
    count_before = cursor.fetchone()[0]
    
    cursor.executemany(insert_sql, records)
    
    # Đếm sau khi insert
    cursor.execute("SELECT COUNT(*) FROM fact_player_match")
    count_after = cursor.fetchone()[0]
    
    inserted = count_after - count_before
    skipped = len(records) - inserted
    print(f" {inserted} new, {skipped} skipped, {count_after} total in DB")




if __name__ == "__main__":
    print("ETL Football - Load Process (Incremental with UPSERT)")
    
    # Load config và kết nối database
    config = load_config()
    conn = connect(config)
    
    if conn is None:
        print("Cannot connect to database")
        exit(1)
    
    conn.autocommit = True
    cursor = conn.cursor()
    
    try:
        # Load dim tables trước (vì fact tables có foreign keys)
        print("\nLoading dimension tables with UPSERT...")
        load_dim_stadium(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_stadium.csv'))
        load_dim_team(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_team.csv'))
        load_dim_match(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_match.csv'))
        load_dim_player(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_player.csv'))
        load_dim_season(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_season.csv'))
        
        # Load fact tables sau với UPSERT
        print("\nLoading fact tables with UPSERT...")
        fact_team_match(cursor, os.path.join(DATA_PROCESSED_DIR, 'fact_team_match_clean.csv'))
        load_fact_team_point(cursor, os.path.join(DATA_PROCESSED_DIR, 'fact_team_point.csv'))
        fact_player_match(cursor, os.path.join(DATA_PROCESSED_DIR, 'fact_player_match_clean.csv'))
        
        print("\nLoad completed")
    finally:
 
        cursor.close()
        conn.close()