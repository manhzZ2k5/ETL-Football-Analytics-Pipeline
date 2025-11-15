
from configparser import ConfigParser
import psycopg2
import os
from pathlib import Path

# Xác định BASE_DIR: ưu tiên environment variable, nếu không thì dùng relative path từ script
if "ETL_FOOTBALL_BASE_DIR" in os.environ:
    BASE_DIR = os.environ["ETL_FOOTBALL_BASE_DIR"]
else:
    # Lấy thư mục chứa script (scr/), rồi lên 1 level để có BASE_DIR
    BASE_DIR = str(Path(__file__).parent.parent.absolute())

DATA_PROCESSED_DIR = os.path.join(BASE_DIR, "data_processed")

# Tạo thư mục nếu chưa tồn tại
os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)


def load_config(filename='database.ini', section='postgresql'):
    """Load database configuration from ini file"""
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
    conn = None
    try:
        conn = psycopg2.connect(**config)
        print("Connected to PostgreSQL server.")
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error connecting to database: {error}")
        return None


def create_and_load_dim_stadium(cursor, csv_path):
    print("Creating Dim_Stadium table:")
    
    sql = '''
    CREATE TABLE IF NOT EXISTS Dim_Stadium (
        stadium_id INT PRIMARY KEY,
        stadium_name VARCHAR(255) NOT NULL,
        capacity INT
    );
    '''
    cursor.execute(sql)
    
    print(f"Loading data from {csv_path}...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        cursor.copy_expert("""
            COPY Dim_Stadium(stadium_id, stadium_name, capacity)
            FROM STDIN
            WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE)
        """, f)
    
    cursor.execute('SELECT COUNT(*) FROM Dim_Stadium;')
    count = cursor.fetchone()[0]
    print(f"Dim_Stadium loaded: {count} records")


def create_and_load_dim_team(cursor, csv_path):
    """Tạo và load dữ liệu vào bảng Dim_Team"""
    print("Creating Dim_Team table...")
    
    sql = '''
    CREATE TABLE IF NOT EXISTS Dim_Team (
        team_id INT PRIMARY KEY,
        team_name VARCHAR(255) NOT NULL, 
        founded_year INT, 
        stadium_id INT,
        shot_name VARCHAR(20),
        FOREIGN KEY (stadium_id) REFERENCES Dim_Stadium(stadium_id)
    );
    '''
    cursor.execute(sql)
    
    print(f"Loading data from {csv_path}...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        cursor.copy_expert("""
            COPY Dim_team(team_id,team_name,founded_year,stadium_id,shot_name)
            FROM STDIN
            WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE)
        """, f)
    
    cursor.execute('SELECT COUNT(*) FROM Dim_Team;')
    count = cursor.fetchone()[0]
    print(f"Dim_Team loaded: {count} records")


def create_and_load_dim_match(cursor, csv_path):
    print("Creating Dim_match table:")
    
    sql = '''
    CREATE TABLE IF NOT EXISTS Dim_match (
        game_id INT PRIMARY KEY,
        game  VARCHAR(255) NOT NULL, 
        game_date date
    );
    '''
    cursor.execute(sql)
    
    print(f"Loading data from {csv_path}...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        cursor.copy_expert("""
            COPY Dim_match(game_id, game, game_date)
            FROM STDIN
            WITH CSV DELIMITER ',' HEADER
        """, f)
    
    cursor.execute('SELECT COUNT(*) FROM Dim_match;')
    count = cursor.fetchone()[0]
    print(f"Dim_match loaded: {count} records")


def create_and_load_dim_player(cursor, csv_path):
    print("Creating Dim_player table:")
    
    sql = '''
    CREATE TABLE IF NOT EXISTS Dim_player (
        player_id INT PRIMARY KEY,
        player_name  VARCHAR(255) NOT NULL, 
        pos CHAR(10),
        nation VARCHAR(20),
        born INT
    );
    '''
    cursor.execute(sql)
    
    print(f"Loading data from {csv_path}...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        cursor.copy_expert("""
            COPY Dim_player(player_id, player_name, pos, nation, born)
            FROM STDIN
            WITH (
                FORMAT CSV,
                DELIMITER ',',
                HEADER TRUE
            )
        """, f)
    
    cursor.execute('SELECT COUNT(*) FROM Dim_player;')
    count = cursor.fetchone()[0]
    print(f"Dim_player loaded: {count} records")


def create_and_load_dim_season(cursor, csv_path):
    print("Creating Dim_season table:")
    
    sql = '''
    CREATE TABLE IF NOT EXISTS Dim_season (
        season_id INT PRIMARY KEY,
        season_name VARCHAR(15) UNIQUE,
        start_year INT,
        end_year INT,
        actual_start_date DATE,
        actual_end_date DATE
    );
    '''
    cursor.execute(sql)
    
    print(f"Loading data from {csv_path}...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        cursor.copy_expert("""
            COPY Dim_season(season_id,season_name, start_year,end_year,actual_start_date,actual_end_date)
            FROM STDIN
            WITH (
                FORMAT CSV,
                DELIMITER ',',
                HEADER TRUE
            )
        """, f)
    
    cursor.execute('SELECT COUNT(*) FROM Dim_season;')
    count = cursor.fetchone()[0]
    print(f"Dim_season loaded: {count} records")


def create_and_load_fact_team_match(cursor, csv_path):
    print("Creating fact_team_match table:")
    
    sql = '''
    CREATE TABLE IF NOT EXISTS fact_team_match (
        season          INT,
        game_id         INT,
        team_id         INT,
        opponent_id     INT,
        round           INT,
        venue           VARCHAR(10),
        result          CHAR(1),
        GF              INT,
        GA              INT,
        xG              NUMERIC(4,2),
        xGA             NUMERIC(4,2),
        Poss            INT,
        captain_id      INT,
        Formation       VARCHAR(20),
        Opp_Formation   VARCHAR(20),
        PRIMARY KEY (season, game_id, team_id),
        FOREIGN KEY (season) REFERENCES dim_season(season_id),
        FOREIGN KEY (game_id) REFERENCES dim_match(game_id),
        FOREIGN KEY (team_id) REFERENCES dim_team(team_id),
        FOREIGN KEY (opponent_id) REFERENCES dim_team(team_id),
        FOREIGN KEY (captain_id) REFERENCES dim_player(player_id)
    );
    '''
    cursor.execute(sql)
    
    print(f"Loading data from {csv_path}...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        cursor.copy_expert("""
            COPY fact_team_match(season,game_id,team_id,opponent_id,round,venue,result,GF,GA,xG,xGA,Poss,captain_id,Formation,Opp_Formation)
            FROM STDIN
            WITH (
                FORMAT CSV,
                DELIMITER ',',
                HEADER TRUE
            )
        """, f)
    
    cursor.execute('SELECT COUNT(*) FROM fact_team_match;')
    count = cursor.fetchone()[0]
    print(f"fact_team_match loaded: {count} records")


def create_and_load_fact_player_match(cursor, csv_path):
    print("Creating fact_player_match table:")
    
    sql = '''
    CREATE TABLE IF NOT EXISTS fact_player_match (
        season                  INT,
        game_id                 INT,
        team_id                 INT,
        player_id               INT,
        min_played              INT,
        goals                   INT,
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
        FOREIGN KEY (game_id)     REFERENCES dim_match(game_id),
        FOREIGN KEY (team_id)     REFERENCES dim_team(team_id),
        FOREIGN KEY (player_id)   REFERENCES dim_player(player_id)
    );
    '''
    cursor.execute(sql)
    
    print(f"Loading data from {csv_path}...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        cursor.copy_expert("""
            COPY fact_player_match(season,game_id,team_id,player_id,min_played,goals,assists,penalty_made,penalty_attempted,shots,shots_on_target,yellow_cards,red_cards,touches,tackles,interceptions,blocks,shot_creating_actions,goal_creating_actions,passes_completed,passes_attempted,pass_completion_percent,progressive_passes,carries,progressive_carries,take_ons_attempted,take_ons_successful)
            FROM STDIN
            WITH (
                FORMAT CSV,
                DELIMITER ',',
                HEADER TRUE
            )
        """, f)
    
    cursor.execute('SELECT COUNT(*) FROM fact_player_match;')
    count = cursor.fetchone()[0]
    print(f"fact_player_match loaded: {count} records")


def create_and_load_fact_team_point(cursor, csv_path):
    print("Creating fact_team_point table:")
    
    sql = '''
    CREATE TABLE IF NOT EXISTS fact_team_point (
        season_id      INT NOT NULL,
        Match_Category VARCHAR(100) NOT NULL,
        Rank           INT NOT NULL,
        team_id        INT NOT NULL,
        MP             INT,
        W              INT,
        D              INT,
        L              INT,
        GF             INT,
        GA             INT,
        GD             INT,
        Pts            INT,
        Recent_Form    VARCHAR(50),
        PRIMARY KEY (season_id, team_id, Match_Category),
        FOREIGN KEY (season_id) REFERENCES dim_season(season_id),
        FOREIGN KEY (team_id)   REFERENCES dim_team(team_id)
    );
    '''
    cursor.execute(sql)
    
    print(f"Loading data from {csv_path}...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        cursor.copy_expert("""
            COPY fact_team_point(season_id,Match_Category,Rank,team_id,MP,W,D,L,GF,GA,GD,Pts,Recent_Form)
            FROM STDIN
            WITH (
                FORMAT CSV,
                DELIMITER ',',
                HEADER TRUE
            )
        """, f)
    
    cursor.execute('SELECT COUNT(*) FROM fact_team_point;')
    count = cursor.fetchone()[0]
    print(f"fact_team_point loaded: {count} records")


if __name__ == "__main__":

    print("ETL Football - Load Process")

    # Load config và kết nối database
    config = load_config()
    conn = connect(config)
    
    if conn is None:
        print("Cannot connect ")
        exit(1)
    
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Load dimension tables trước (vì fact tables có foreign keys)
    create_and_load_dim_stadium(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_stadium.csv'))
    create_and_load_dim_team(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_team.csv'))
    create_and_load_dim_match(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_match.csv'))
    create_and_load_dim_player(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_player.csv'))
    create_and_load_dim_season(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_season.csv'))
    
    # Load fact tables sau
    create_and_load_fact_team_match(cursor, os.path.join(DATA_PROCESSED_DIR, 'fact_team_match_clean.csv'))
    create_and_load_fact_player_match(cursor, os.path.join(DATA_PROCESSED_DIR, 'fact_player_match_clean.csv'))
    create_and_load_fact_team_point(cursor, os.path.join(DATA_PROCESSED_DIR, 'fact_team_point.csv'))
    
    # Đóng kết nối
    cursor.close()
    conn.close()
    
  
    print("Load process completed")

