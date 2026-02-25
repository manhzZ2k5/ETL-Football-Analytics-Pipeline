# ⚽ ETL Football Analytics Pipeline

> **Automated end-to-end data pipeline** that extracts Premier League statistics from FBref & Flashfootball, transforms them into a Star Schema data warehouse, loads into PostgreSQL, and serves interactive analytics through a Streamlit dashboard — all orchestrated by Apache Airflow and containerized with Docker.

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Tech Stack](#-tech-stack)
- [Architecture](#-architecture)
- [Data Sources](#-data-sources)
- [Project Structure](#-project-structure)
- [Database Schema](#-database-schema)
- [Dashboard Features](#-dashboard-features)
- [Quick Start](#-quick-start)
- [ETL Pipeline Details](#-etl-pipeline-details)
- [Airflow Orchestration](#-airflow-orchestration)
- [Configuration](#-configuration)

---

## 🔍 Overview

This project builds a **production-grade ETL pipeline** for Premier League football data, covering **5+ seasons (2020–21 → 2024–25)**. Key highlights:

- 📦 **Extracts** player & team statistics from **FBref** (via `soccerdata` API) and standings from **Flashfootball** (via **Selenium** web scraping)
- 🔄 **Transforms** raw data into a clean **Star Schema** data warehouse with 4 dimension tables and 3 fact tables
- 💾 **Loads** into **PostgreSQL** using idempotent **UPSERT** logic — safe to re-run without duplicating data
- 📊 **Visualizes** with an interactive **Streamlit + Plotly** dashboard
- ⚙️ **Schedules** via **Apache Airflow** DAG (weekly, every Wednesday at 2 AM)
- 🐳 **Containerized** with Docker Compose for one-command deployment

---

## 🛠 Tech Stack

| Layer | Technology |
|---|---|
| **Language** | Python 3.11 |
| **Data Extraction** | `soccerdata`, `Selenium`, `webdriver-manager`, `BeautifulSoup4`, `Playwright` |
| **Data Processing** | `pandas`, `numpy` |
| **Data Warehouse** | PostgreSQL 15 |
| **ORM / DB Driver** | `psycopg2` |
| **Orchestration** | Apache Airflow |
| **Dashboard** | Streamlit, Plotly (Express & Graph Objects) |
| **Containerization** | Docker, Docker Compose |
| **Dependency Mgmt** | Poetry + `pyproject.toml` |

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     APACHE AIRFLOW (Scheduler)                  │
│              Cron: Every Wednesday 2 AM (0 2 * * 3)            │
└───────────────────────┬─────────────────────────────────────────┘
                        │
          ┌─────────────▼──────────────┐
          │        EXTRACT PHASE       │
          │  ┌────────────────────┐    │
          │  │  FBref (soccerdata)│    │  ← Player season stats
          │  │  API Library       │    │  ← Player match stats
          │  └────────────────────┘    │  ← Team match stats
          │  ┌────────────────────┐    │  ← Team season stats
          │  │ Flashfootball.com  │    │
          │  │ (Selenium Scraper) │    │  ← League standings
          │  └────────────────────┘    │    (Overall/Home/Away)
          └─────────────┬──────────────┘
                        │ data_raw/ (CSV)
          ┌─────────────▼──────────────┐
          │       TRANSFORM PHASE      │
          │  Star Schema Modeling      │
          │  ┌──────────┬───────────┐  │
          │  │dim_player│dim_team   │  │
          │  │dim_match │dim_stadium│  │
          │  │dim_season│           │  │
          │  ├──────────┴───────────┤  │
          │  │fact_team_match       │  │
          │  │fact_player_match     │  │
          │  │fact_team_point       │  │
          │  └──────────────────────┘  │
          └─────────────┬──────────────┘
                        │ data_processed/ (CSV)
          ┌─────────────▼──────────────┐
          │         LOAD PHASE         │
          │  PostgreSQL (UPSERT)       │
          │  ON CONFLICT DO NOTHING /  │
          │  ON CONFLICT DO UPDATE     │
          └─────────────┬──────────────┘
                        │
          ┌─────────────▼──────────────┐
          │      STREAMLIT DASHBOARD   │
          │  plotly charts + analytics │
          └────────────────────────────┘
```

---

## 📡 Data Sources

### 1. FBref via `soccerdata` library
Retrieves structured, multi-season Premier League statistics:
- **Player Season Stats** → Aggregated per-player per-season metrics
- **Player Match Stats** → Per-match player performance (goals, assists, xG, xA, passes, carries, take-ons, etc.)
- **Team Match Stats** → Per-team per-match results (GF, GA, xG, xGA, possession, formation)
- **Team Season Stats** → Annual aggregated team metrics

### 2. Flashfootball.com via Selenium
Scrapes Premier League standings for **6 seasons (2020–21 to 2025–26)**:
- **Overall / Home / Away** standings separately
- Stats: Rank, MP, W, D, L, GF, GA, GD, Points, Recent Form

**Incremental logic**: On first run, all seasons are extracted. Subsequent runs only re-scrape the current season, controlled via `.last_extract_date.txt`.

---

## 📁 Project Structure

```
ETL_Football/
│
├── scr/                          # Core ETL scripts
│   ├── Extract.py                # Data extraction (FBref API + Selenium scraping)
│   ├── Transform.py              # Data cleaning & Star Schema modeling
│   ├── Load.py                   # PostgreSQL loading with UPSERT
│   └── ui.py                     # Streamlit analytics dashboard
│
├── dags/
│   └── football_etl_dag.py       # Apache Airflow DAG definition
│
├── docker/
│   └── initdb/
│       └── 10-create-databases.sql  # DB initialization script
│
├── data_raw/                     # Raw CSV files from extraction
│   ├── fbref_fact_player_season_stats.csv
│   ├── fbref_fact_player_match_stats.csv
│   ├── fbref_fact_team_match.csv
│   ├── fbref_fact_team_season.csv
│   ├── team_point.csv            # Scraped from Flashfootball
│   ├── dim_team.csv              # Manual team reference data
│   └── dim_stadium.csv           # Manual stadium reference data
│
├── data_processed/               # Transformed, clean CSV files
│   ├── dim_player.csv
│   ├── dim_team.csv
│   ├── dim_match.csv
│   ├── dim_stadium.csv
│   ├── dim_season.csv
│   ├── fact_team_match_clean.csv
│   ├── fact_player_match_clean.csv
│   └── fact_team_point.csv
│
├── Dockerfile                    # Container for ETL + Dashboard
├── docker-compose.yml            # Orchestrates all 3 services
├── pyproject.toml                # Poetry project config
├── requirements.txt              # pip dependencies
└── .gitignore
```

---

## 🗄 Database Schema

The data warehouse follows a **Star Schema** design for efficient analytical queries.

### Dimension Tables

```sql
Dim_Season      (season_id PK, season_name, start_year, end_year, actual_start_date, actual_end_date)
Dim_Team        (team_id PK, team_name, founded_year, stadium_id FK, short_name)
Dim_Stadium     (stadium_id PK, stadium_name, capacity)
Dim_Player      (player_id PK, player_name, pos, nation, born)
Dim_Match       (match_id PK, match_name, match_date)
```

### Fact Tables

```sql
fact_team_match       (season, game_id, team_id, opponent_id, round, venue, result,
                        GF, GA, xG, xGA, Poss, Formation, Opp_Formation)
                        PRIMARY KEY (season, game_id, team_id)

fact_player_match     (season, game_id, team_id, player_id, min_played, goals, xG, xA,
                        assists, penalty_made, penalty_attempted, shots, shots_on_target,
                        yellow_cards, red_cards, touches, tackles, interceptions, blocks,
                        shot_creating_actions, goal_creating_actions, passes_completed,
                        passes_attempted, pass_completion_percent, progressive_passes,
                        carries, progressive_carries, take_ons_attempted, take_ons_successful)
                        PRIMARY KEY (season, game_id, team_id, player_id)

Fact_Team_Point       (season_id, Match_Category, Rank, team_id,
                        MP, W, D, L, GF, GA, GD, Pts, Recent_Form)
                        PRIMARY KEY (season_id, team_id, Match_Category)
```

> All foreign keys are enforced at the database level. Fact tables use `ON CONFLICT DO NOTHING` (idempotent inserts), while dimension tables use `ON CONFLICT DO UPDATE` (upsert).

---

## 📊 Dashboard Features

Built with **Streamlit** and **Plotly**, the dashboard is organized into 4 interactive tabs:

| Tab | Features |
|---|---|
| **Season Overview** | League standings table (color-coded: green for top 4, red for bottom 3), Top scorers chart, Top assisters chart, xG vs Actual Goals scatter plot |
| **Team Analysis** | Team KPIs (rank, points, W-D-L), Top 5 team scorers, Last 5 matches form timeline, Home vs Away performance comparison |
| **Trends & Comparison** | Multi-season goal trend (dual-axis), Home advantage ranking, Home vs Away scatter plot |
| **Advanced Analysis** | Attack vs Defense 4-quadrant matrix, Top 5 / Bottom 5 teams table, Offensive ranking bar chart, Defensive ranking bar chart |

**All charts are interactive** with hover tooltips, zoom, and filter controls.

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose installed
- Git

### Option 1: Docker Compose (Recommended)

```bash
# Clone the repository
git clone <repo-url>
cd ETL_Football

# Start all services (PostgreSQL + ETL Worker + Dashboard)
docker-compose up --build

# Dashboard will be available at:
http://localhost:8501
```

The `etl-worker` service runs the full ETL pipeline automatically on startup:
```
python scr/Extract.py && python scr/Transform.py && python scr/Load.py
```

### Option 2: Local Development

**1. Install dependencies**
```bash
# Using pip
pip install -r requirements.txt

# Or using Poetry
poetry install
```

**2. Configure database connection**

Edit `scr/database.ini`:
```ini
[postgresql]
host=localhost
port=5432
database=ETL_FOOTBALL
user=football_user
password=your_password
```

**3. Run the ETL pipeline manually**
```bash
# Step 1: Extract
python scr/Extract.py

# Step 2: Transform
python scr/Transform.py

# Step 3: Load
python scr/Load.py
```

**4. Launch the dashboard**
```bash
streamlit run scr/ui.py
```

---

## 🔄 ETL Pipeline Details

### Extract (`scr/Extract.py`)

**Incremental extraction logic:**
- Checks `.last_extract_date.txt` to determine if this is a first or subsequent run
- **First run**: Fetches all 5 seasons (2021–2425)
- **Subsequent runs**: Only fetches the current season (2024–25)

**Web scraping with Selenium:**
- Headless Chrome with `ChromeDriverManager` (auto-installs the correct ChromeDriver version)
- Explicit WebDriverWait for dynamic table loading
- Custom user-agent to avoid bot detection

**Incremental merge strategy:**
- New data is merged with existing raw CSVs using a key-based upsert (`merge_with_existing_raw_data`)
- Old records with matching keys are replaced; unique old records are preserved

### Transform (`scr/Transform.py`)

Each function creates one table:

| Function | Output |
|---|---|
| `create_dim_player()` | Combines season + match stats; deduplicates players |
| `create_dim_team()` | Standardizes team names, adds short codes (e.g. `ARS`, `LIV`) |
| `create_dim_stadium()` | Cleans stadium data, handles malformed rows |
| `create_dim_match()` | Extracts unique games, validates dates |
| `create_fact_team_match()` | Joins with all dimensions; normalizes team names |
| `create_fact_player_match()` | Maps player/team/game IDs; handles 30+ stats columns |
| `create_fact_team_point()` | Converts Flashfootball standings; splits GF:GA field |

**Handles real-world data quality issues:**
- Multi-level (MultiIndex) CSV headers from FBref
- Team name variants (`Manchester United` → `Manchester Utd`, `Wolverhampton Wanderers` → `Wolves`)
- Suffix cleanup (`F.C.`, `A.F.C.`, `AFC` removal)
- Malformed / misaligned CSV rows

### Load (`scr/Load.py`)

- Creates tables `IF NOT EXISTS` — idempotent schema management
- **Dimension tables**: `ON CONFLICT DO UPDATE` (upsert — always reflects latest data)
- **Fact tables**: `ON CONFLICT DO NOTHING` (skip duplicates — preserve historical data)
- Docker-aware connection: detects container environment and switches `localhost` → `host.docker.internal`
- Logs `inserted / skipped / total` counts for each table

---

## ⚙️ Airflow Orchestration

**DAG:** `football_etl_pipeline`  
**Schedule:** Every Wednesday at 2 AM (`0 2 * * 3`)  
**Retries:** 2 attempts, 5-minute delay

```
start ──► extract_data ──► transform_data ──► load_data
```

Each task is a `PythonOperator` that imports and runs the corresponding module functions. The DAG respects `ETL_FOOTBALL_BASE_DIR` env variable, allowing it to run both locally and inside Docker.

---

## ⚙️ Configuration

### Environment Variables (Docker)

| Variable | Default | Description |
|---|---|---|
| `ETL_FOOTBALL_BASE_DIR` | `/app` | Root directory of the project |
| `DB_HOST` | `postgres` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_USER` | `football_user` | DB username |
| `DB_PASSWORD` | `12345` | DB password |
| `DB_NAME` | `ETL_FOOTBALL` | Database name |

### `scr/database.ini`

```ini
[postgresql]
host=localhost
port=5432
database=ETL_FOOTBALL
user=football_user
password=12345
```

---

## 📌 Key Design Decisions

- **Incremental loading**: The pipeline is designed to run repeatedly without re-fetching all historical data, saving time and avoiding rate limiting.
- **Idempotent loads**: Using `ON CONFLICT` clauses means the load step is always safe to re-run — no manual cleanup needed.
- **Flexible column resolution**: The `_get_column()` helper in `Transform.py` handles both single-level and MultiIndex CSV headers from FBref, making the pipeline resilient to FBref format changes.
- **Dockerized Selenium**: The Dockerfile includes all necessary system libraries (`libatk-bridge2.0-0`, `libgtk-3-0`, etc.) for headless Chrome to run inside a container.

---

## 📄 License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
