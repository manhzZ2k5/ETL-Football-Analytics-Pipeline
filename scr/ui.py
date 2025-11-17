
# import streamlit as st
# import pandas as pd
# import psycopg2
# from configparser import ConfigParser
# import plotly.express as px
# import os
# import matplotlib

# # --- CẤU HÌNH VÀ KẾT NỐI DATABASE ---

# @st.cache_resource
# def load_config(filename: str = 'database.ini', section: str = 'postgresql') -> dict:
#     """Load DB config from filename in current dir or script dir."""
#     # locate file: prefer environment var ETL_FOOTBALL_BASE_DIR then current working dir
#     base_dir = os.environ.get('ETL_FOOTBALL_BASE_DIR', os.getcwd())
#     file_path = os.path.join(base_dir, filename)

#     if not os.path.exists(file_path):
#         # fall back to this file's directory
#         file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)

#     if not os.path.exists(file_path):
#         raise FileNotFoundError(f"Config file not found: {file_path}")

#     parser = ConfigParser()
#     parser.read(file_path)

#     if not parser.has_section(section):
#         raise Exception(f"Section '{section}' not found in {file_path}. Available: {parser.sections()}")

#     return {k: v for k, v in parser.items(section)}


# @st.cache_resource
# def get_connection(config: dict):
#     """Return a psycopg2 connection based on config dict."""
#     conn = psycopg2.connect(**config)
#     return conn
# config = load_config()
# conn = get_connection(config=config)

# # --- CÁC HÀM LẤY DỮ LIỆU (SỬ DỤNG CACHE ĐỂ TĂNG TỐC) ---

# @st.cache_data(ttl=600) # Cache dữ liệu trong 10 phút
# def get_seasons():
#     """Lấy danh sách các mùa giải"""
#     df = pd.read_sql('SELECT season_name FROM dim_season ORDER BY season_name DESC', conn)
#     return df['season_name'].tolist()

# @st.cache_data(ttl=600)
# def get_league_table(season_name):
#     """Lấy bảng xếp hạng của một mùa giải"""
#     query = """
#         SELECT 
#             ftp.Rank,
#             dt.team_name AS "Đội bóng",
#             ftp.mp AS "Trận",
#             ftp.w AS "Thắng",
#             ftp.d AS "Hòa",
#             ftp.l AS "Thua",
#             ftp.gf AS "BT",
#             ftp.ga AS "BB",
#             ftp.gd AS "HS",
#             ftp.pts AS "Điểm"
#         FROM fact_team_point ftp
#         JOIN dim_team dt ON ftp.team_id = dt.team_id
#         JOIN dim_season ds ON ftp.season_id = ds.season_id
#         WHERE ds.season_name = %s
#           AND ftp.Match_Category = 'overall'
#         ORDER BY ftp.Rank;
#     """
#     df = pd.read_sql(query, conn, params=(season_name,))
#     return df

# @st.cache_data(ttl=600)
# def get_top_scorers(season_name, limit=10):
#     """Lấy top cầu thủ ghi bàn"""
#     query = """
#         SELECT 
#             dp.player_name,
#             dt.team_name,
#             SUM(fpm.goals) as total_goals
#         FROM fact_player_match fpm
#         JOIN dim_player dp ON fpm.player_id = dp.player_id
#         JOIN dim_team dt ON fpm.team_id = dt.team_id
#         JOIN dim_season ds ON fpm.season = ds.season_id
#         WHERE ds.season_name = %s
#         GROUP BY dp.player_name, dt.team_name
#         HAVING SUM(fpm.goals) > 0
#         ORDER BY total_goals DESC
#         LIMIT %s;
#     """
#     df = pd.read_sql(query, conn, params=(season_name, limit))
#     return df

# @st.cache_data(ttl=600)
# def get_top_assisters(season_name, limit=10):
#     """Lấy top cầu thủ kiến tạo"""
#     query = """
#         SELECT 
#             dp.player_name,
#             dt.team_name,
#             SUM(fpm.assists) as total_assists
#         FROM fact_player_match fpm
#         JOIN dim_player dp ON fpm.player_id = dp.player_id
#         JOIN dim_team dt ON fpm.team_id = dt.team_id
#         JOIN dim_season ds ON fpm.season = ds.season_id
#         WHERE ds.season_name = %s
#         GROUP BY dp.player_name, dt.team_name
#         HAVING SUM(fpm.assists) > 0
#         ORDER BY total_assists DESC
#         LIMIT %s;
#     """
#     df = pd.read_sql(query, conn, params=(season_name, limit))
#     return df

# @st.cache_data(ttl=600)
# def get_season_overview_stats(season_name):
#     """Lấy các thống kê tổng quan của mùa giải, xử lý trường hợp không có dữ liệu."""
#     query = """
#         SELECT
#             COALESCE(COUNT(DISTINCT ftm.game_id), 0) as total_matches,
#             COALESCE(SUM(ftm.GF), 0) as total_goals
#         FROM fact_team_match ftm
#         JOIN dim_season ds ON ftm.season = ds.season_id
#         WHERE ds.season_name = %s;
#     """
#     df = pd.read_sql(query, conn, params=(season_name,))
#     return df.iloc[0]
# # --- CÁC HÀM LẤY DỮ LIỆU MỚI ---
# @st.cache_data(ttl=600)
# def get_teams(season_name):
#     """Lấy danh sách các đội tham gia trong một mùa giải"""
#     query = """
#         SELECT DISTINCT dt.team_name
#         FROM fact_team_point ftp
#         JOIN dim_team dt ON ftp.team_id = dt.team_id
#         JOIN dim_season ds ON ftp.season_id = ds.season_id
#         WHERE ds.season_name = %s
#         ORDER BY dt.team_name;
#     """
#     df = pd.read_sql(query, conn, params=(season_name,))
#     return df['team_name'].tolist()

# @st.cache_data(ttl=600)
# def get_team_kpis(season_name, team_name):
#     """Lấy các chỉ số chính của một đội"""
#     query = """
#         SELECT ftp.w, ftp.d, ftp.l, ftp.gf, ftp.ga, ftp.pts
#         FROM fact_team_point ftp
#         JOIN dim_team dt ON ftp.team_id = dt.team_id
#         JOIN dim_season ds ON ftp.season_id = ds.season_id
#         WHERE ds.season_name = %s AND dt.team_name = %s AND LOWER(ftp.match_category) = 'overall'
#     """
#     df = pd.read_sql(query, conn, params=(season_name, team_name))
#     return df.iloc[0] if not df.empty else None

# @st.cache_data(ttl=600)
# def get_team_top_scorers(season_name, team_name, limit=5):
#     """Lấy top cầu thủ ghi bàn của một đội"""
#     query = """
#         SELECT dp.player_name, SUM(fpm.goals) as total_goals
#         FROM fact_player_match fpm
#         JOIN dim_player dp ON fpm.player_id = dp.player_id
#         JOIN dim_team dt ON fpm.team_id = dt.team_id
#         JOIN dim_season ds ON fpm.season = ds.season_id
#         WHERE ds.season_name = %s AND dt.team_name = %s
#         GROUP BY dp.player_name
#         HAVING SUM(fpm.goals) > 0
#         ORDER BY total_goals DESC
#         LIMIT %s;
#     """
#     return pd.read_sql(query, conn, params=(season_name, team_name, limit))
# @st.cache_data(ttl=600)
# def get_xg_vs_goals_data(season_name):
#     query = """
#         SELECT 
#             dt.team_name,
#             SUM(ftm.gf) as total_goals,
#             SUM(ftm.xg) as total_xg
#         FROM fact_team_match ftm
#         JOIN dim_team dt ON ftm.team_id = dt.team_id
#         JOIN dim_season ds ON ftm.season = ds.season_id
#         WHERE ds.season_name = %s
#         GROUP BY dt.team_name;
#     """
#     df = pd.read_sql(query, conn, params=(season_name,))
#     df['performance'] = df['total_goals'] - df['total_xg']
#     return df
# # --- GIAO DIỆN DASHBOARD ---

# st.set_page_config(page_title="Football Analytics Dashboard", layout="wide")

# st.title("⚽ Dashboard Phân Tích Dữ Liệu Bóng Đá")
# st.markdown("Một cái nhìn tổng quan về các mùa giải dựa trên dữ liệu bạn đã xử lý.")

# # --- SIDEBAR FILTERS ---
# st.sidebar.header("Bộ lọc")
# seasons_list = get_seasons()
# if seasons_list:
#     selected_season = st.sidebar.selectbox("Chọn mùa giải", seasons_list)
# else:
#     st.sidebar.warning("Không tìm thấy dữ liệu mùa giải.")
#     st.stop()


# # --- HIỂN THỊ DỮ LIỆU THEO MÙA GIẢI ĐÃ CHỌN ---
# st.header(f"Tổng quan mùa giải: {selected_season}")

# # Các thẻ thống kê chính (KPIs)
# overview_stats = get_season_overview_stats(selected_season)
# total_matches = overview_stats['total_matches']
# total_goals = overview_stats['total_goals']
# goals_per_match = round(total_goals / total_matches, 2) if total_matches > 0 else 0

# col1, col2, col3 = st.columns(3)
# col1.metric("Tổng số trận đấu", f"{total_matches}")
# col2.metric("Tổng số bàn thắng", f"{int(total_goals)}")
# col3.metric("Bàn thắng / Trận", f"{goals_per_match}")

# st.markdown("---")

# # Bảng xếp hạng
# st.subheader(f"Bảng xếp hạng mùa giải {selected_season}")
# # Trong phần hiển thị bảng xếp hạng
# league_table_df = get_league_table(selected_season)
# st.dataframe(
#     league_table_df.style.background_gradient(cmap='Greens', subset=['Điểm', 'BT'])
#                              .apply(lambda x: ['background-color: #FFDDC1' if x.name < 4 else '' for i in x], axis=1),
#     use_container_width=True
# )
# # Dòng .apply ở trên sẽ tô màu nền cho top 4 đội đầu bảng

# st.markdown("---")

# # Biểu đồ Top cầu thủ
# col_scorers, col_assisters = st.columns(2)

# with col_scorers:
#     st.subheader("Vua phá lưới")
#     top_scorers_df = get_top_scorers(selected_season)
#     if not top_scorers_df.empty:
#         fig_scorers = px.bar(top_scorers_df.sort_values('total_goals', ascending=True),
#                              x='total_goals',
#                              y='player_name',
#                              orientation='h',
#                              title=f"Top 10 cầu thủ ghi bàn hàng đầu",
#                              labels={'player_name': 'Cầu thủ', 'total_goals': 'Số bàn thắng'},
#                              text='total_goals',
#                              hover_data=['team_name'])
#         fig_scorers.update_layout(yaxis={'categoryorder':'total ascending'})
#         st.plotly_chart(fig_scorers, use_container_width=True)
#     else:
#         st.warning("Không có dữ liệu Vua phá lưới cho mùa giải này.")


# with col_assisters:
#     st.subheader("Vua kiến tạo")
#     top_assisters_df = get_top_assisters(selected_season)
#     if not top_assisters_df.empty:
#         fig_assisters = px.bar(top_assisters_df.sort_values('total_assists', ascending=True),
#                                x='total_assists',
#                                y='player_name',
#                                orientation='h',
#                                title=f"Top 10 cầu thủ kiến tạo hàng đầu",
#                                labels={'player_name': 'Cầu thủ', 'total_assists': 'Số kiến tạo'},
#                                text='total_assists',
#                                hover_data=['team_name'],
#                                color_discrete_sequence=px.colors.sequential.Viridis)
#         fig_assisters.update_layout(yaxis={'categoryorder':'total ascending'})
#         st.plotly_chart(fig_assisters, use_container_width=True)
#     else:
#         st.warning("Không có dữ liệu Vua kiến tạo cho mùa giải này.")

# # Footer
# st.sidebar.markdown("---")
# # st.sidebar.info("Dashboard được xây dựng bằng Streamlit, Python và PostgreSQL.")
# # --- TRONG PHẦN GIAO DIỆN DASHBOARD ---
# st.title("⚽ Dashboard Phân Tích Dữ Liệu Bóng Đá")

# # ... (sidebar của bạn giữ nguyên) ...

# tab1, tab2 = st.tabs(["📊 Tổng quan mùa giải", "🏆 Phân tích đội bóng"])

# with tab1:
#     # --- Toàn bộ code hiển thị tổng quan mùa giải của bạn (bảng xếp hạng, top scorers...) nằm ở đây ---
#     st.header(f"Tổng quan mùa giải: {selected_season}")
#     # ... dán code cũ vào đây ...

# with tab2:
#     st.header(f"Phân tích chi tiết Đội bóng trong mùa giải {selected_season}")
    
#     teams_list = get_teams(selected_season)
#     if teams_list:
#         selected_team = st.selectbox("Chọn một đội bóng", teams_list)

#         st.subheader(f"Thành tích của {selected_team}")
#         team_kpis = get_team_kpis(selected_season, selected_team)

#         if team_kpis is not None:
#             col1, col2, col3, col4 = st.columns(4)
#             col1.metric("Bàn thắng (GF)", team_kpis['gf'])
#             col2.metric("Bàn thua (GA)", team_kpis['ga'])
#             col3.metric("Điểm số (Pts)", team_kpis['pts'])
#             col4.metric("Thắng-Hòa-Thua", f"{team_kpis['w']}-{team_kpis['d']}-{team_kpis['l']}")

#             st.markdown("---")
            
#             st.subheader("Cầu thủ ghi bàn hàng đầu")
#             team_scorers_df = get_team_top_scorers(selected_season, selected_team)
#             if not team_scorers_df.empty:
#                 fig = px.bar(team_scorers_df, x='player_name', y='total_goals',
#                              title=f"Top 5 cầu thủ ghi bàn của {selected_team}",
#                              labels={'player_name': 'Cầu thủ', 'total_goals': 'Số bàn thắng'},
#                              text='total_goals')
#                 st.plotly_chart(fig, use_container_width=True)
#             else:
#                 st.info(f"{selected_team} không có cầu thủ nào ghi bàn trong mùa giải này.")
#         else:
#             st.warning(f"Không tìm thấy dữ liệu KPI cho {selected_team}.")
#     else:
#         st.warning("Không có dữ liệu đội bóng cho mùa giải này.")
# # --- Trong `with tab1:` ---
# st.markdown("---")
# st.subheader("Hiệu quả dứt điểm (Bàn thắng thực tế vs. Bàn thắng kỳ vọng)")
# xg_df = get_xg_vs_goals_data(selected_season)
# if not xg_df.empty:
#     fig_xg = px.scatter(xg_df, x='total_xg', y='total_goals',
#                         text='team_name',  # Hiển thị tên đội
#                         title='So sánh hiệu suất tấn công của các đội',
#                         labels={'total_xg': 'Tổng bàn thắng kỳ vọng (xG)', 'total_goals': 'Tổng bàn thắng thực tế (GF)'},
#                         hover_data=['performance'])
    
#     # Vẽ đường chéo y=x để dễ so sánh
#     fig_xg.add_shape(type='line', x0=xg_df['total_xg'].min(), y0=xg_df['total_xg'].min(),
#                      x1=xg_df['total_xg'].max(), y1=xg_df['total_xg'].max(),
#                      line=dict(color='Gray', dash='dash'))

#     fig_xg.update_traces(textposition='top center')
#     st.plotly_chart(fig_xg, use_container_width=True)
#     st.caption("Các đội nằm phía trên đường nét đứt dứt điểm hiệu quả hơn kỳ vọng, và ngược lại.")
import streamlit as st
import pandas as pd
import psycopg2
from configparser import ConfigParser
import plotly.express as px
import plotly.graph_objects as go
import os

# --- CẤU HÌNH VÀ KẾT NỐI DATABASE ---

@st.cache_resource
def load_config(filename: str = 'database.ini', section: str = 'postgresql') -> dict:
    """Load DB config from filename in current dir or script dir."""
    base_dir = os.environ.get('ETL_FOOTBALL_BASE_DIR', os.getcwd())
    file_path = os.path.join(base_dir, filename)

    if not os.path.exists(file_path):
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Config file not found: {file_path}")

    parser = ConfigParser()
    parser.read(file_path)

    if not parser.has_section(section):
        raise Exception(f"Section '{section}' not found in {file_path}. Available: {parser.sections()}")

    return {k: v for k, v in parser.items(section)}


@st.cache_resource
def get_connection(config: dict):
    """Return a psycopg2 connection based on config dict."""
    conn = psycopg2.connect(**config)
    return conn

config = load_config()
conn = get_connection(config=config)

# --- CÁC HÀM LẤY DỮ LIỆU ---

@st.cache_data(ttl=600)
def get_seasons():
    """Lấy danh sách các mùa giải"""
    df = pd.read_sql('SELECT season_name FROM dim_season ORDER BY season_name DESC', conn)
    return df['season_name'].tolist()

@st.cache_data(ttl=600)
def get_league_table(season_name):
    """Lấy bảng xếp hạng của một mùa giải"""
    query = """
        SELECT 
            ftp.Rank,
            dt.team_name AS "Đội bóng",
            ftp.mp AS "Trận",
            ftp.w AS "Thắng",
            ftp.d AS "Hòa",
            ftp.l AS "Thua",
            ftp.gf AS "BT",
            ftp.ga AS "BB",
            ftp.gd AS "HS",
            ftp.pts AS "Điểm"
        FROM fact_team_point ftp
        JOIN dim_team dt ON ftp.team_id = dt.team_id
        JOIN dim_season ds ON ftp.season_id = ds.season_id
        WHERE ds.season_name = %s
          AND ftp.Match_Category = 'overall'
        ORDER BY ftp.Rank;
    """
    df = pd.read_sql(query, conn, params=(season_name,))
    return df

@st.cache_data(ttl=600)
def get_top_scorers(season_name, limit=10):
    """Lấy top cầu thủ ghi bàn"""
    query = """
        SELECT 
            dp.player_name,
            dt.team_name,
            SUM(fpm.goals) as total_goals
        FROM fact_player_match fpm
        JOIN dim_player dp ON fpm.player_id = dp.player_id
        JOIN dim_team dt ON fpm.team_id = dt.team_id
        JOIN dim_season ds ON fpm.season = ds.season_id
        WHERE ds.season_name = %s
        GROUP BY dp.player_name, dt.team_name
        HAVING SUM(fpm.goals) > 0
        ORDER BY total_goals DESC
        LIMIT %s;
    """
    df = pd.read_sql(query, conn, params=(season_name, limit))
    return df

@st.cache_data(ttl=600)
def get_top_assisters(season_name, limit=10):
    """Lấy top cầu thủ kiến tạo"""
    query = """
        SELECT 
            dp.player_name,
            dt.team_name,
            SUM(fpm.assists) as total_assists
        FROM fact_player_match fpm
        JOIN dim_player dp ON fpm.player_id = dp.player_id
        JOIN dim_team dt ON fpm.team_id = dt.team_id
        JOIN dim_season ds ON fpm.season = ds.season_id
        WHERE ds.season_name = %s
        GROUP BY dp.player_name, dt.team_name
        HAVING SUM(fpm.assists) > 0
        ORDER BY total_assists DESC
        LIMIT %s;
    """
    df = pd.read_sql(query, conn, params=(season_name, limit))
    return df

@st.cache_data(ttl=600)
def get_season_overview_stats(season_name):
    """Lấy các thống kê tổng quan của mùa giải"""
    query = """
        SELECT
            COALESCE(COUNT(DISTINCT ftm.game_id), 0) as total_matches,
            COALESCE(SUM(ftm.GF), 0) as total_goals
        FROM fact_team_match ftm
        JOIN dim_season ds ON ftm.season = ds.season_id
        WHERE ds.season_name = %s;
    """
    df = pd.read_sql(query, conn, params=(season_name,))
    return df.iloc[0]

@st.cache_data(ttl=600)
def get_teams(season_name):
    """Lấy danh sách các đội tham gia trong một mùa giải"""
    query = """
        SELECT DISTINCT dt.team_name
        FROM fact_team_point ftp
        JOIN dim_team dt ON ftp.team_id = dt.team_id
        JOIN dim_season ds ON ftp.season_id = ds.season_id
        WHERE ds.season_name = %s
        ORDER BY dt.team_name;
    """
    df = pd.read_sql(query, conn, params=(season_name,))
    return df['team_name'].tolist()

@st.cache_data(ttl=600)
def get_team_kpis(season_name, team_name):
    """Lấy các chỉ số chính của một đội"""
    query = """
        SELECT ftp.w, ftp.d, ftp.l, ftp.gf, ftp.ga, ftp.pts, ftp.rank
        FROM fact_team_point ftp
        JOIN dim_team dt ON ftp.team_id = dt.team_id
        JOIN dim_season ds ON ftp.season_id = ds.season_id
        WHERE ds.season_name = %s AND dt.team_name = %s AND LOWER(ftp.match_category) = 'overall'
    """
    df = pd.read_sql(query, conn, params=(season_name, team_name))
    return df.iloc[0] if not df.empty else None

@st.cache_data(ttl=600)
def get_team_top_scorers(season_name, team_name, limit=5):
    """Lấy top cầu thủ ghi bàn của một đội"""
    query = """
        SELECT dp.player_name, SUM(fpm.goals) as total_goals
        FROM fact_player_match fpm
        JOIN dim_player dp ON fpm.player_id = dp.player_id
        JOIN dim_team dt ON fpm.team_id = dt.team_id
        JOIN dim_season ds ON fpm.season = ds.season_id
        WHERE ds.season_name = %s AND dt.team_name = %s
        GROUP BY dp.player_name
        HAVING SUM(fpm.goals) > 0
        ORDER BY total_goals DESC
        LIMIT %s;
    """
    return pd.read_sql(query, conn, params=(season_name, team_name, limit))

@st.cache_data(ttl=600)
def get_xg_vs_goals_data(season_name):
    """Lấy dữ liệu xG vs Goals thực tế"""
    query = """
        SELECT 
            dt.team_name,
            SUM(ftm.gf) as total_goals,
            SUM(ftm.xg) as total_xg
        FROM fact_team_match ftm
        JOIN dim_team dt ON ftm.team_id = dt.team_id
        JOIN dim_season ds ON ftm.season = ds.season_id
        WHERE ds.season_name = %s
        GROUP BY dt.team_name;
    """
    df = pd.read_sql(query, conn, params=(season_name,))
    df['performance'] = df['total_goals'] - df['total_xg']
    return df

# --- HÀM MỚI: PHÂN TÍCH HOME/AWAY ---
@st.cache_data(ttl=600)
def get_home_away_performance(season_name):
    """
    Lấy hiệu suất sân nhà/sân khách (sử dụng subquery để đảm bảo alias hoạt động trong ORDER BY).
    """
    query = """
        SELECT *
        FROM (
            SELECT 
                dt.team_name,
                SUM(CASE WHEN LOWER(ftp.match_category) = 'home' THEN ftp.pts ELSE 0 END) as home_pts,
                SUM(CASE WHEN LOWER(ftp.match_category) = 'away' THEN ftp.pts ELSE 0 END) as away_pts,
                SUM(CASE WHEN LOWER(ftp.match_category) = 'home' THEN ftp.w ELSE 0 END) as home_wins,
                SUM(CASE WHEN LOWER(ftp.match_category) = 'away' THEN ftp.w ELSE 0 END) as away_wins
            FROM fact_team_point ftp
            JOIN dim_team dt ON ftp.team_id = dt.team_id
            JOIN dim_season ds ON ftp.season_id = ds.season_id
            WHERE ds.season_name = %s AND LOWER(ftp.match_category) IN ('home', 'away')
            GROUP BY dt.team_name
        ) AS performance_summary
        ORDER BY (performance_summary.home_pts + performance_summary.away_pts) DESC;
    """
    return pd.read_sql(query, conn, params=(season_name,))

# --- HÀM MỚI: PHÂN TÍCH PHÒNG NGỰ ---
@st.cache_data(ttl=600)
def get_defensive_stats(season_name):
    """Lấy thống kê phòng ngự"""
    query = """
        SELECT 
            dt.team_name,
            ftp.ga as goals_conceded,
            ftp.mp as matches_played,
            ROUND(CAST(ftp.ga AS DECIMAL) / NULLIF(ftp.mp, 0), 2) as avg_goals_conceded
        FROM fact_team_point ftp
        JOIN dim_team dt ON ftp.team_id = dt.team_id
        JOIN dim_season ds ON ftp.season_id = ds.season_id
        WHERE ds.season_name = %s AND LOWER(ftp.match_category) = 'overall'
        ORDER BY avg_goals_conceded ASC;
    """
    return pd.read_sql(query, conn, params=(season_name,))

# --- HÀM MỚI: PHÂN TÍCH TẤN CÔNG ---
@st.cache_data(ttl=600)
def get_offensive_stats(season_name):
    """Lấy thống kê tấn công"""
    query = """
        SELECT 
            dt.team_name,
            ftp.gf as goals_scored,
            ftp.mp as matches_played,
            ROUND(CAST(ftp.gf AS DECIMAL) / NULLIF(ftp.mp, 0), 2) as avg_goals_scored
        FROM fact_team_point ftp
        JOIN dim_team dt ON ftp.team_id = dt.team_id
        JOIN dim_season ds ON ftp.season_id = ds.season_id
        WHERE ds.season_name = %s AND LOWER(ftp.match_category) = 'overall'
        ORDER BY avg_goals_scored DESC;
    """
    return pd.read_sql(query, conn, params=(season_name,))

# --- HÀM MỚI: SO SÁNH CÁC MÙA GIẢI ---
@st.cache_data(ttl=600)
def get_season_comparison():
    """So sánh các mùa giải"""
    query = """
        SELECT 
            ds.season_name,
            COUNT(DISTINCT ftm.game_id) as total_matches,
            SUM(ftm.gf) as total_goals,
            ROUND(CAST(SUM(ftm.gf) AS DECIMAL) / NULLIF(COUNT(DISTINCT ftm.game_id), 0), 2) as avg_goals_per_match
        FROM fact_team_match ftm
        JOIN dim_season ds ON ftm.season = ds.season_id
        GROUP BY ds.season_name
        ORDER BY ds.season_name DESC;
    """
    return pd.read_sql(query, conn)

# --- HÀM MỚI: FORM GẦN ĐÂY (5 TRẬN) ---
@st.cache_data(ttl=600)
def get_team_recent_form(season_name, team_name, limit=5):
    """Lấy phong độ gần đây của đội (đã sửa lỗi tên cột và logic)."""
    query = """
        SELECT 
            dm.game_date,                            -- SỬA LỖI 1: dm.date -> dm.game_date
            o_dt.team_name as opponent_name,
            ftm.venue,
            ftm.result,
            ftm.gf as goals_for,
            ftm.ga as goals_against
        FROM fact_team_match ftm
        JOIN dim_team dt ON ftm.team_id = dt.team_id
        JOIN dim_team o_dt ON ftm.opponent_id = o_dt.team_id -- Join thêm 1 lần để lấy tên đối thủ
        JOIN dim_season ds ON ftm.season = ds.season_id
        JOIN dim_match dm ON ftm.game_id = dm.game_id
        WHERE ds.season_name = %s AND dt.team_name = %s
        ORDER BY dm.game_date DESC                    -- SỬA LỖI 1: dm.date -> dm.game_date
        LIMIT %s;
    """
    return pd.read_sql(query, conn, params=(season_name, team_name, limit))

# --- HÀM MỚI: PHÂN TÍCH TOP/BOTTOM ---
@st.cache_data(ttl=600)
def get_top_bottom_performers(season_name):
    """Lấy đội xuất sắc nhất và kém nhất theo nhiều tiêu chí"""
    query = """
        SELECT 
            dt.team_name,
            ftp.pts,
            ftp.gf,
            ftp.ga,
            ftp.gd,
            ftp.w,
            ftp.d,
            ftp.l
        FROM fact_team_point ftp
        JOIN dim_team dt ON ftp.team_id = dt.team_id
        JOIN dim_season ds ON ftp.season_id = ds.season_id
        WHERE ds.season_name = %s AND LOWER(ftp.match_category) = 'overall'
        ORDER BY ftp.pts DESC;
    """
    return pd.read_sql(query, conn, params=(season_name,))

# --- GIAO DIỆN DASHBOARD ---

st.set_page_config(page_title="Football Analytics Dashboard", layout="wide", initial_sidebar_state="expanded")

st.title("Dashboard Phân Tích Dữ Liệu Bóng Đá")
st.markdown("Phân tích toàn diện về các mùa giải và đội bóng")

# --- SIDEBAR ---
st.sidebar.header("Bộ lọc")
seasons_list = get_seasons()
if seasons_list:
    selected_season = st.sidebar.selectbox("Chọn mùa giải", seasons_list)
else:
    st.sidebar.warning("Không tìm thấy dữ liệu mùa giải.")
    st.stop()

st.sidebar.markdown("---")
st.sidebar.markdown("### Thống kê nhanh")

# --- TABS CHÍNH ---
tab1, tab2, tab3, tab4 = st.tabs([
    "Tổng quan mùa giải", 
    "Phân tích đội bóng",
    "So sánh & Xu hướng",
    "Phân tích nâng cao"
])

# ==================== TAB 1: TỔNG QUAN MÙA GIẢI ====================
with tab1:
    st.header(f"Tổng quan mùa giải: {selected_season}")
    
    # KPIs
    overview_stats = get_season_overview_stats(selected_season)
    total_matches = overview_stats['total_matches']
    total_goals = overview_stats['total_goals']
    goals_per_match = round(total_goals / total_matches, 2) if total_matches > 0 else 0
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Tổng số trận đấu", f"{total_matches}")
    col2.metric("Tổng số bàn thắng", f"{int(total_goals)}")
    col3.metric("Bàn thắng / Trận", f"{goals_per_match}")
    
    st.markdown("---")
    
    # Bảng xếp hạng
    st.subheader(f"Bảng xếp hạng mùa giải {selected_season}")
    league_table_df = get_league_table(selected_season)
    
    def highlight_top_bottom(row):
        if row.name < 4:
            return ['background-color: #90EE90'] * len(row)  # Top 4: xanh lá
        elif row.name >= len(league_table_df) - 3:
            return ['background-color: #FFB6C1'] * len(row)  # Bottom 3: hồng
        return [''] * len(row)
    
    st.dataframe(
        league_table_df.style.apply(highlight_top_bottom, axis=1)
                             .background_gradient(cmap='Greens', subset=['Điểm', 'BT']),
        use_container_width=True,
        height=600
    )
    
    st.markdown("---")
    
    # Top cầu thủ
    col_scorers, col_assisters = st.columns(2)
    
    with col_scorers:
        st.subheader("Vua phá lưới")
        top_scorers_df = get_top_scorers(selected_season)
        if not top_scorers_df.empty:
            fig_scorers = px.bar(
                top_scorers_df.sort_values('total_goals', ascending=True),
                x='total_goals',
                y='player_name',
                orientation='h',
                title=f"Top 10 cầu thủ ghi bàn",
                labels={'player_name': 'Cầu thủ', 'total_goals': 'Số bàn thắng'},
                text='total_goals',
                hover_data=['team_name'],
                color='total_goals',
                color_continuous_scale='Reds'
            )
            fig_scorers.update_layout(yaxis={'categoryorder':'total ascending'}, showlegend=False)
            st.plotly_chart(fig_scorers, use_container_width=True)
        else:
            st.warning("Không có dữ liệu Vua phá lưới.")
    
    with col_assisters:
        st.subheader("Vua kiến tạo")
        top_assisters_df = get_top_assisters(selected_season)
        if not top_assisters_df.empty:
            fig_assisters = px.bar(
                top_assisters_df.sort_values('total_assists', ascending=True),
                x='total_assists',
                y='player_name',
                orientation='h',
                title=f"Top 10 cầu thủ kiến tạo",
                labels={'player_name': 'Cầu thủ', 'total_assists': 'Số kiến tạo'},
                text='total_assists',
                hover_data=['team_name'],
                color='total_assists',
                color_continuous_scale='Blues'
            )
            fig_assisters.update_layout(yaxis={'categoryorder':'total ascending'}, showlegend=False)
            st.plotly_chart(fig_assisters, use_container_width=True)
        else:
            st.warning("Không có dữ liệu Vua kiến tạo.")
    
    st.markdown("---")
    
    # xG Analysis
    st.subheader("Hiệu quả dứt điểm (Bàn thắng thực tế vs. Kỳ vọng)")
    xg_df = get_xg_vs_goals_data(selected_season)
    if not xg_df.empty:
        fig_xg = px.scatter(
            xg_df, 
            x='total_xg', 
            y='total_goals',
            text='team_name',
            title='So sánh hiệu suất tấn công',
            labels={'total_xg': 'Bàn thắng kỳ vọng (xG)', 'total_goals': 'Bàn thắng thực tế'},
            hover_data=['performance'],
            color='performance',
            color_continuous_scale='RdYlGn',
            size='total_goals'
        )
        
        # Đường chéo y=x
        fig_xg.add_shape(
            type='line', 
            x0=xg_df['total_xg'].min(), 
            y0=xg_df['total_xg'].min(),
            x1=xg_df['total_xg'].max(), 
            y1=xg_df['total_xg'].max(),
            line=dict(color='Gray', dash='dash')
        )
        
        fig_xg.update_traces(textposition='top center')
        st.plotly_chart(fig_xg, use_container_width=True)
        st.caption("💡 Đội nằm trên đường nét đứt: Dứt điểm hiệu quả hơn kỳ vọng")

# ==================== TAB 2: PHÂN TÍCH ĐỘI BÓNG ====================
with tab2:
    st.header(f"🏆 Phân tích chi tiết Đội bóng - {selected_season}")
    
    teams_list = get_teams(selected_season)
    if teams_list:
        selected_team = st.selectbox("Chọn một đội bóng", teams_list, key="team_selector")
        
        team_kpis = get_team_kpis(selected_season, selected_team)
        
        if team_kpis is not None:
            # KPIs đội bóng
            st.subheader(f"Thành tích của {selected_team}")
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Hạng", f"#{int(team_kpis['rank'])}")
            col2.metric("Điểm số", team_kpis['pts'])
            col3.metric("Bàn thắng", team_kpis['gf'])
            col4.metric("Bàn thua", team_kpis['ga'])
            col5.metric("W-D-L", f"{team_kpis['w']}-{team_kpis['d']}-{team_kpis['l']}")
            
            st.markdown("---")
            
            col_left, col_right = st.columns(2)
            
            with col_left:
                # Top scorers
                st.subheader("Cầu thủ ghi bàn hàng đầu")
                team_scorers_df = get_team_top_scorers(selected_season, selected_team)
                if not team_scorers_df.empty:
                    fig = px.bar(
                        team_scorers_df, 
                        x='player_name', 
                        y='total_goals',
                        title=f"Top 5 ghi bàn",
                        labels={'player_name': 'Cầu thủ', 'total_goals': 'Bàn thắng'},
                        text='total_goals',
                        color='total_goals',
                        color_continuous_scale='Oranges'
                    )
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Không có dữ liệu ghi bàn.")
            
            with col_right:
                # Phong độ gần đây
                st.subheader("Phong độ gần đây (5 trận)")
                recent_form = get_team_recent_form(selected_season, selected_team, 5)
                if not recent_form.empty:
                    # Tạo biểu đồ dạng timeline
                    form_colors = {'W': 'green', 'D': 'orange', 'L': 'red'}
                    fig_form = go.Figure()
                    
                    for idx, row in recent_form.iterrows():
                        fig_form.add_trace(go.Bar(
                            x=[row['game_date']],
                            y=[1],
                            name=row['result'],
                            marker_color=form_colors[row['result']],
                            text=f"{row['result']}<br>{row['goals_for']}-{row['goals_against']}",
                            textposition='inside',
                            hovertemplate=f"<b>{row['result']}</b><br>Tỷ số: {row['goals_for']}-{row['goals_against']}<extra></extra>"
                        ))
                    
                    fig_form.update_layout(
                        title="Kết quả 5 trận gần nhất",
                        showlegend=False,
                        yaxis=dict(showticklabels=False, showgrid=False),
                        xaxis_title="Ngày thi đấu"
                    )
                    st.plotly_chart(fig_form, use_container_width=True)
                else:
                    st.info("Không có dữ liệu phong độ gần đây.")
            
            st.markdown("---")
            
            # Phân tích sân nhà/sân khách
            st.subheader("Hiệu suất Sân nhà vs Sân khách")
            home_away_df = get_home_away_performance(selected_season)
            team_home_away = home_away_df[home_away_df['team_name'] == selected_team]
            
            if not team_home_away.empty:
                row = team_home_away.iloc[0]
                
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Điểm sân nhà", row['home_pts'])
                col2.metric("Điểm sân khách", row['away_pts'])
                col3.metric("Thắng sân nhà", row['home_wins'])
                col4.metric("Thắng sân khách", row['away_wins'])
                
                # Biểu đồ so sánh
                comparison_data = pd.DataFrame({
                    'Loại': ['Sân nhà', 'Sân khách'],
                    'Điểm': [row['home_pts'], row['away_pts']],
                    'Thắng': [row['home_wins'], row['away_wins']]
                })
                
                fig_comparison = go.Figure(data=[
                    go.Bar(name='Điểm số', x=comparison_data['Loại'], y=comparison_data['Điểm']),
                    go.Bar(name='Trận thắng', x=comparison_data['Loại'], y=comparison_data['Thắng'])
                ])
                fig_comparison.update_layout(barmode='group', title="So sánh hiệu suất")
                st.plotly_chart(fig_comparison, use_container_width=True)
        else:
            st.warning(f"Không tìm thấy dữ liệu cho {selected_team}.")
    else:
        st.warning("Không có dữ liệu đội bóng.")

# ==================== TAB 3: SO SÁNH & XU HƯỚNG ====================
with tab3:
    st.header("So sánh & Xu hướng các mùa giải")
    
    # So sánh các mùa giải
    season_comparison_df = get_season_comparison()
    if not season_comparison_df.empty:
        st.subheader("Xu hướng qua các mùa giải")
        
        fig_seasons = go.Figure()
        fig_seasons.add_trace(go.Scatter(
            x=season_comparison_df['season_name'],
            y=season_comparison_df['total_goals'],
            mode='lines+markers',
            name='Tổng bàn thắng',
            line=dict(color='red', width=3),
            marker=dict(size=10)
        ))
        
        fig_seasons.add_trace(go.Scatter(
            x=season_comparison_df['season_name'],
            y=season_comparison_df['avg_goals_per_match'],
            mode='lines+markers',
            name='TB bàn thắng/trận',
            line=dict(color='blue', width=3),
            marker=dict(size=10),
            yaxis='y2'
        ))
        
        fig_seasons.update_layout(
            title="Xu hướng bàn thắng qua các mùa giải",
            xaxis_title="Mùa giải",
            yaxis_title="Tổng bàn thắng",
            yaxis2=dict(title="TB bàn thắng/trận", overlaying='y', side='right'),
            hovermode='x unified'
        )
        
        st.plotly_chart(fig_seasons, use_container_width=True)
        
        # Bảng so sánh chi tiết
        st.subheader("Bảng so sánh chi tiết")
        st.dataframe(
            season_comparison_df.style.background_gradient(cmap='YlOrRd', subset=['total_goals', 'avg_goals_per_match']),
            use_container_width=True
        )
    
    st.markdown("---")
    
    # So sánh hiệu suất sân nhà/sân khách
    st.subheader("So sánh hiệu suất Sân nhà vs Sân khách (Toàn giải)")
    home_away_all = get_home_away_performance(selected_season)
    
    if not home_away_all.empty:
        # Top 10 đội có lợi thế sân nhà lớn nhất
        home_away_all['home_advantage'] = home_away_all['home_pts'] - home_away_all['away_pts']
        top_home_advantage = home_away_all.nlargest(10, 'home_advantage')
        
        fig_home_adv = px.bar(
            top_home_advantage.sort_values('home_advantage', ascending=True),
            x='home_advantage',
            y='team_name',
            orientation='h',
            title='Top 10 đội có lợi thế sân nhà lớn nhất',
            labels={'home_advantage': 'Chênh lệch điểm (Nhà - Khách)', 'team_name': 'Đội bóng'},
            color='home_advantage',
            color_continuous_scale='RdYlGn',
            text='home_advantage'
        )
        fig_home_adv.update_layout(yaxis={'categoryorder':'total ascending'}, showlegend=False)
        st.plotly_chart(fig_home_adv, use_container_width=True)
        
        # Scatter plot: Home pts vs Away pts
        fig_scatter_ha = px.scatter(
            home_away_all,
            x='home_pts',
            y='away_pts',
            text='team_name',
            title='Phân bố điểm số: Sân nhà vs Sân khách',
            labels={'home_pts': 'Điểm sân nhà', 'away_pts': 'Điểm sân khách'},
            color='home_advantage',
            color_continuous_scale='RdYlGn',
            size='home_wins'
        )
        
        # Đường chéo y=x
        max_pts = max(home_away_all['home_pts'].max(), home_away_all['away_pts'].max())
        fig_scatter_ha.add_shape(
            type='line',
            x0=0, y0=0,
            x1=max_pts, y1=max_pts,
            line=dict(color='gray', dash='dash')
        )
        
        fig_scatter_ha.update_traces(textposition='top center')
        st.plotly_chart(fig_scatter_ha, use_container_width=True)
        st.caption("Đội nằm trên đường nét đứt: Hiệu suất sân khách tốt hơn sân nhà")

# ==================== TAB 4: PHÂN TÍCH NÂNG CAO ====================
with tab4:
    st.header("Phân tích nâng cao")
    
    # Phân tích tấn công vs phòng ngự
    st.subheader("Ma trận Tấn công vs Phòng ngự")
    
    offensive_df = get_offensive_stats(selected_season)
    defensive_df = get_defensive_stats(selected_season)
    
    if not offensive_df.empty and not defensive_df.empty:
        # Merge dữ liệu
        attack_defense = offensive_df.merge(
            defensive_df[['team_name', 'avg_goals_conceded']], 
            on='team_name'
        )
        
        # Scatter plot 4 quadrant
        fig_quad = px.scatter(
            attack_defense,
            x='avg_goals_scored',
            y='avg_goals_conceded',
            text='team_name',
            title='Ma trận Tấn công vs Phòng ngự (TB bàn thắng/trận)',
            labels={
                'avg_goals_scored': 'TB bàn thắng ghi được/trận',
                'avg_goals_conceded': 'TB bàn thua/trận'
            },
            size='goals_scored',
            color='goals_scored',
            color_continuous_scale='RdYlGn'
        )
        
        # Thêm đường phân chia 4 quadrant
        avg_attack = attack_defense['avg_goals_scored'].mean()
        avg_defense = attack_defense['avg_goals_conceded'].mean()
        
        fig_quad.add_hline(y=avg_defense, line_dash="dash", line_color="gray", opacity=0.5)
        fig_quad.add_vline(x=avg_attack, line_dash="dash", line_color="gray", opacity=0.5)
        
        # Thêm annotations cho các quadrant
        fig_quad.add_annotation(
            x=attack_defense['avg_goals_scored'].max() * 0.95,
            y=attack_defense['avg_goals_conceded'].min() * 1.1,
            text="<b>Elite</b><br>(Tấn công mạnh, Phòng ngự tốt)",
            showarrow=False,
            bgcolor="lightgreen",
            opacity=0.7
        )
        
        fig_quad.add_annotation(
            x=attack_defense['avg_goals_scored'].min() * 1.1,
            y=attack_defense['avg_goals_conceded'].max() * 0.95,
            text="<b>Yếu</b><br>(Tấn công kém, Phòng ngự kém)",
            showarrow=False,
            bgcolor="lightcoral",
            opacity=0.7
        )
        
        fig_quad.update_traces(textposition='top center')
        st.plotly_chart(fig_quad, use_container_width=True)
        st.caption("Góc phần tư phải trên: Đội có cả tấn công và phòng ngự tốt")
    
    st.markdown("---")
    
    # Top & Bottom performers
    st.subheader("Đội xuất sắc nhất & Kém nhất")
    
    performers_df = get_top_bottom_performers(selected_season)
    
    if not performers_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Top 5 Xuất sắc nhất")
            top_5 = performers_df.head(5)[['team_name', 'pts', 'gf', 'ga', 'w']]
            top_5.columns = ['Đội', 'Điểm', 'BT', 'BB', 'Thắng']
            st.dataframe(
                top_5.style.background_gradient(cmap='Greens'),
                use_container_width=True,
                hide_index=True
            )
        
        with col2:
            st.markdown("#### Top 5 Kém nhất")
            bottom_5 = performers_df.tail(5)[['team_name', 'pts', 'gf', 'ga', 'l']]
            bottom_5.columns = ['Đội', 'Điểm', 'BT', 'BB', 'Thua']
            st.dataframe(
                bottom_5.style.background_gradient(cmap='Reds_r'),
                use_container_width=True,
                hide_index=True
            )
    
    st.markdown("---")
    
    # Phân tích hiệu suất tấn công
    st.subheader("Bảng xếp hạng Tấn công")
    if not offensive_df.empty:
        top_attack = offensive_df.head(10)
        
        fig_attack = go.Figure()
        fig_attack.add_trace(go.Bar(
            y=top_attack['team_name'],
            x=top_attack['goals_scored'],
            name='Tổng bàn thắng',
            orientation='h',
            marker_color='crimson',
            text=top_attack['goals_scored']
        ))
        
        fig_attack.update_layout(
            title='Top 10 đội tấn công mạnh nhất',
            xaxis_title='Tổng bàn thắng',
            yaxis_title='Đội bóng',
            yaxis={'categoryorder':'total ascending'},
            height=500
        )
        
        st.plotly_chart(fig_attack, use_container_width=True)
        
        # Thêm bảng chi tiết
        st.dataframe(
            offensive_df.head(10).style.background_gradient(cmap='Reds', subset=['goals_scored', 'avg_goals_scored']),
            use_container_width=True
        )
    
    st.markdown("---")
    
    # Phân tích phòng ngự
    st.subheader("Bảng xếp hạng Phòng ngự")
    if not defensive_df.empty:
        top_defense = defensive_df.head(10)
        
        fig_defense = go.Figure()
        fig_defense.add_trace(go.Bar(
            y=top_defense['team_name'],
            x=top_defense['goals_conceded'],
            name='Tổng bàn thua',
            orientation='h',
            marker_color='steelblue',
            text=top_defense['goals_conceded']
        ))
        
        fig_defense.update_layout(
            title='Top 10 đội phòng ngự tốt nhất (ít bàn thua nhất)',
            xaxis_title='Tổng bàn thua',
            yaxis_title='Đội bóng',
            yaxis={'categoryorder':'total descending'},
            height=500
        )
        
        st.plotly_chart(fig_defense, use_container_width=True)
        
        # Thêm bảng chi tiết
        st.dataframe(
            defensive_df.head(10).style.background_gradient(cmap='Blues_r', subset=['goals_conceded', 'avg_goals_conceded']),
            use_container_width=True
        )
    
    st.markdown("---")
    
    # Phân tích Consistency (Hệ số biến thiên)
    st.subheader("Phân tích độ ổn định")
    st.info("Phần này phân tích mức độ ổn định của các đội qua các trận đấu")
    
    # Tính toán win rate
    if not performers_df.empty:
        performers_df['win_rate'] = (performers_df['w'] / (performers_df['w'] + performers_df['d'] + performers_df['l']) * 100).round(1)
        performers_df['points_per_game'] = (performers_df['pts'] / (performers_df['w'] + performers_df['d'] + performers_df['l'])).round(2)
        
        # Scatter: Win rate vs Points per game
        fig_consistency = px.scatter(
            performers_df,
            x='win_rate',
            y='points_per_game',
            text='team_name',
            title='Tỷ lệ thắng vs Điểm trung bình/trận',
            labels={'win_rate': 'Tỷ lệ thắng (%)', 'points_per_game': 'Điểm TB/trận'},
            color='pts',
            color_continuous_scale='Viridis',
            size='gf'
        )
        fig_consistency.update_traces(textposition='top center')
        st.plotly_chart(fig_consistency, use_container_width=True)


st.sidebar.success(f"Đã tải dữ liệu mùa giải: **{selected_season}**")