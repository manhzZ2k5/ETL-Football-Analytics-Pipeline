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

    return config

def connect(config):
    """Kết nối tới PostgreSQL và trả về đối tượng connection"""
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
    # Xóa dữ liệu cũ (vì có foreign keys, cần xóa fact tables trước)
    cursor.execute("TRUNCATE TABLE Dim_Stadium CASCADE;")
    
    # CSV has typo 'statium_name' but database has 'stadium_name'
    # Read CSV and rename column
    df = pd.read_csv(csv_path)
    df_renamed = df.rename(columns={'statium_name': 'stadium_name'})
    
    # Write to StringIO buffer for COPY
    buffer = StringIO()
    df_renamed.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    
    cursor.copy_expert("""
        COPY Dim_Stadium(stadium_id, stadium_name, capacity)
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER ',')
    """, buffer)
    print("dim_stadium loaded successfully")


def load_dim_team(cursor, csv_path):
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
    # Xóa dữ liệu cũ
    cursor.execute("TRUNCATE TABLE Dim_Team CASCADE;")
    
    # Read CSV and load data
    df = pd.read_csv(csv_path)
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    
    cursor.copy_expert("""
        COPY Dim_Team(team_id, team_name, founded_year, stadium_id, short_name)
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER ',')
    """, buffer)
    print("dim_team loaded successfully")


def load_dim_match(cursor, csv_path):
    sql1 = '''
    CREATE TABLE IF NOT EXISTS Dim_Match (
        match_id INT PRIMARY KEY,
        match_name VARCHAR(255) NOT NULL,
        match_date DATE
    );
    '''
    cursor.execute(sql1)
    # Xóa dữ liệu cũ
    cursor.execute("TRUNCATE TABLE Dim_Match CASCADE;")
    
    # CSV has 5 columns but we only need 3: game_id, game, date (mapped to game_date)
    # Read CSV and select only needed columns
    df = pd.read_csv(csv_path)
    df_selected = df[['game_id', 'game', 'date']].copy()
    df_selected.rename(columns={'date': 'game_date', 'game': 'match_name', 'game_id': 'match_id'}, inplace=True)
    
    # Write to StringIO buffer for COPY
    buffer = StringIO()
    df_selected.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    
    cursor.copy_expert("""
        COPY Dim_Match(match_id, match_name, match_date)
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER ',')
    """, buffer)
    print("dim_match loaded successfully")


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
    # Xóa dữ liệu cũ
    cursor.execute("TRUNCATE TABLE Dim_Player CASCADE;")
    
    # CSV has column 'player' but database has 'player_name'
    # Read CSV and rename column
    df = pd.read_csv(csv_path)
    df_renamed = df.rename(columns={'player': 'player_name'})
    
    # Chuyển đổi cột 'born' từ float sang integer (pandas có thể đọc thành float)
    if 'born' in df_renamed.columns:
        # Chuyển về Int64 (nullable integer) để xử lý NaN
        df_renamed['born'] = pd.to_numeric(df_renamed['born'], errors='coerce').astype('Int64')
    
    # Write to StringIO buffer for COPY (na_rep='' để NaN thành empty string → NULL trong DB)
    buffer = StringIO()
    df_renamed.to_csv(buffer, index=False, header=False, na_rep='')
    buffer.seek(0)
    
    cursor.copy_expert("""
        COPY Dim_Player(player_id, player_name, pos, nation, born)
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER ',')
    """, buffer)
    print("dim_player loaded successfully")

def load_dim_season(cursor, csv_path):
    sql1 = '''
    CREATE TABLE IF NOT EXISTS Dim_season (
        season_id INT PRIMARY KEY,
        season_name VARCHAR(15) UNIQUE,
        start_year INT,
        end_year INT,
        actual_start_date DATE ,
        actual_end_date DATE
    );
    ''' 
    cursor.execute(sql1)
    # Xóa dữ liệu cũ
    cursor.execute("TRUNCATE TABLE Dim_season CASCADE;")
    
    df = pd.read_csv(csv_path)
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    
    cursor.copy_expert("""
        COPY Dim_season(season_id, season_name, start_year, end_year, actual_start_date, actual_end_date)
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER ',')
    """, buffer)
    print("dim_season loaded successfully")

def fact_team_match(cursor, csv_path):
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
    captain_id      INT,
    "Formation"     VARCHAR(20),
    "Opp Formation" VARCHAR(20),

    -- Khóa chính đề xuất
    PRIMARY KEY (season, game_id, team_id),

    -- FOREIGN KEYS
    FOREIGN KEY (season)
        REFERENCES dim_season(season_id),

    FOREIGN KEY (game_id)
        REFERENCES dim_match(match_id),

    FOREIGN KEY (team_id)
        REFERENCES dim_team(team_id),

    FOREIGN KEY (opponent_id)
        REFERENCES dim_team(team_id),

    FOREIGN KEY (captain_id)
        REFERENCES dim_player(player_id)
);
    '''
    cursor.execute(sql1)
    # Xóa dữ liệu cũ
    cursor.execute("TRUNCATE TABLE fact_team_match;")
    
    df = pd.read_csv(csv_path)
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    
    cursor.copy_expert("""
        COPY fact_team_match(season,game_id,team_id,opponent_id,round,venue,result,"GF","GA","xG","xGA","Poss",captain_id,"Formation","Opp Formation")
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER ',')
    """, buffer)
    print("fact_team_match loaded successfully")

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
    # Xóa dữ liệu cũ
    cursor.execute("TRUNCATE TABLE Fact_Team_Point;")
    
    df = pd.read_csv(csv_path)
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep='')
    buffer.seek(0)
    
    cursor.copy_expert("""
        COPY Fact_Team_Point(season_id, "Match_Category", "Rank", team_id, "MP", "W", "D", "L", "GF", "GA", "GD", "Pts", "Recent_Form")
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER ',')
    """, buffer)
    print("fact_team_point loaded successfully")

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


    FOREIGN KEY (season)      REFERENCES dim_season(season_id),
    FOREIGN KEY (game_id)     REFERENCES dim_match(match_id),
    FOREIGN KEY (team_id)     REFERENCES dim_team(team_id),
    FOREIGN KEY (player_id)   REFERENCES dim_player(player_id)
);
    '''
    cursor.execute(sql1)
    # Xóa dữ liệu cũ
    cursor.execute("TRUNCATE TABLE fact_player_match;")
    
    df = pd.read_csv(csv_path)
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    
    cursor.copy_expert("""
        COPY fact_player_match(season,game_id,team_id,player_id,min_played,goals,xG,xA,assists,penalty_made,penalty_attempted,shots,shots_on_target,yellow_cards,red_cards,touches,tackles,interceptions,blocks,shot_creating_actions,goal_creating_actions,passes_completed,passes_attempted,pass_completion_percent,progressive_passes,carries,progressive_carries,take_ons_attempted,take_ons_successful)
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER ',')
    """, buffer)
    print("fact_player_match loaded successfully")




if __name__ == "__main__":
    print("ETL Football - Load Process")
    
    # Load config và kết nối database
    config = load_config()
    conn = connect(config)
    
    if conn is None:
        print("Cannot connect to database")
        exit(1)
    
    conn.autocommit = True
    cursor = conn.cursor()
    
    try:
        # Load dimension tables trước (vì fact tables có foreign keys)
        load_dim_stadium(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_stadium.csv'))
        load_dim_team(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_team.csv'))
        load_dim_match(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_match.csv'))
        load_dim_player(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_player.csv'))
        load_dim_season(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_season.csv'))
        
        # Load fact tables sau
        fact_team_match(cursor, os.path.join(DATA_PROCESSED_DIR, 'fact_team_match_clean.csv'))
        load_fact_team_point(cursor, os.path.join(DATA_PROCESSED_DIR, 'fact_team_point.csv'))
        fact_player_match(cursor, os.path.join(DATA_PROCESSED_DIR, 'fact_player_match_clean.csv'))
        
        print("\nLoad process completed successfully!")
    finally:
        # Đóng kết nối
        cursor.close()
        conn.close()
