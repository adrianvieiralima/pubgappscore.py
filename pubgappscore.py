import streamlit as st
import pandas as pd
import requests
import psycopg2
import os
from datetime import datetime

# =============================
# CONFIGURAÇÃO DA PÁGINA
# =============================
st.set_page_config(
    page_title="PUBG Squad Ranking",
    layout="wide",
    page_icon="🏆",
    initial_sidebar_state="collapsed"
)

# =============================
# CSS TEMA ESCURO CUSTOM
# =============================
st.markdown("""
<style>
    .stApp { background-color: #0e1117; color: white; }
    div[data-testid="stMetric"] { background-color: #161b22; padding: 15px; border-radius: 12px; border: 1px solid #30363d; text-align: center; }
    [data-testid="stMetricLabel"] * { font-size: 40px !important; }
    [data-testid="stMetricValue"] { font-size: 38px !important; }
    div[data-testid="stTabs"] button { font-size: 16px; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# ==========================================================
# LÓGICA DE ATUALIZAÇÃO (SILENCIOSA)
# ==========================================================

def atualizar_dados_pubg_api():
    """Busca dados na API e atualiza o banco de dados"""
    try:
        # Tenta pegar das secrets (Streamlit Cloud) ou do ambiente (Local)
        API_KEY = st.secrets.get("PUBG_API_KEY")
        DATABASE_URL = st.secrets.get("DATABASE_URL")
        
        if not API_KEY or not DATABASE_URL:
            return

        headers = {"Authorization": f"Bearer {API_KEY}", "Accept": "application/vnd.api+json"}
        BASE_URL = "https://api.pubg.com/shards/steam"

        # 1. Detectar Temporada
        res_season = requests.get(f"{BASE_URL}/seasons", headers=headers).json()
        current_season_id = next(s["id"] for s in res_season["data"] if s["attributes"]["isCurrentSeason"])

        # 2. Lista de Jogadores
        players_names = [
            "Adrian-Wan", "MironoteuCool", "FabioEspeto", "Mamutag_Komander",
            "Robson_Foz", "MEIRAA", "EL-LOCORJ", "SalaminhoKBD",
            "nelio_ponto_dev", "CARNEIROOO", "Kowalski_PR", "Zacouteguy",
            "Sidors", "Takato_Matsuki", "cmm01", "Petrala",
            "Fumiga_BR", "O-CARRASCO"
        ]
        
        # 3. Mapeamento de IDs
        mapping_id_name = {}
        for i in range(0, len(players_names), 10):
            grupo = ",".join(players_names[i:i+10])
            res = requests.get(f"{BASE_URL}/players?filter[playerNames]={grupo}", headers=headers)
            if res.status_code == 200:
                for p in res.json()["data"]:
                    mapping_id_name[p["id"]] = p["attributes"]["name"]

        # 4. Buscar Stats e Salvar
        resultados = []
        ids_list = list(mapping_id_name.keys())
        for i in range(0, len(ids_list), 10):
            grupo_ids = ",".join(ids_list[i:i+10])
            url_stats = f"{BASE_URL}/seasons/{current_season_id}/gameMode/squad/players?filter[playerIds]={grupo_ids}"
            res = requests.get(url_stats, headers=headers)
            if res.status_code == 200:
                for p_data in res.json().get("data", []):
                    p_id = p_data["relationships"]["player"]["data"]["id"]
                    stats = p_data["attributes"]["gameModeStats"]
                    partidas = stats.get("roundsPlayed", 0)
                    if partidas > 0:
                        resultados.append((
                            mapping_id_name.get(p_id), partidas, round(stats.get("kills", 0)/partidas, 2),
                            stats.get("wins", 0), stats.get("kills", 0), int(stats.get("damageDealt", 0)/partidas),
                            stats.get("assists", 0), stats.get("headshotKills", 0), stats.get("revives", 0),
                            stats.get("longestKill", 0.0), datetime.utcnow()
                        ))

        if resultados:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            sql = """
                INSERT INTO ranking_squad (nick, partidas, kr, vitorias, kills, dano_medio, assists, headshots, revives, kill_dist_max, atualizado_em)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (nick) DO UPDATE SET 
                partidas=EXCLUDED.partidas, kr=EXCLUDED.kr, vitorias=EXCLUDED.vitorias, kills=EXCLUDED.kills, 
                dano_medio=EXCLUDED.dano_medio, assists=EXCLUDED.assists, headshots=EXCLUDED.headshots, 
                revives=EXCLUDED.revives, kill_dist_max=EXCLUDED.kill_dist_max, atualizado_em=EXCLUDED.atualizado_em
            """
            cur.executemany(sql, resultados)
            conn.commit()
            cur.close()
            conn.close()
    except Exception:
        pass # Falha silenciosa para não travar o app

# =============================
# CONEXÃO E CACHE
# =============================

@st.cache_data(ttl=300)
def get_data(table_name):
    # Roda a atualização antes de buscar (apenas a cada 5 min)
    if table_name == "v_ranking_squad_completo":
        atualizar_dados_pubg_api()
        
    try:
        conn = st.connection("postgresql", type="sql", url=st.secrets["DATABASE_URL"])
        return conn.query(f"SELECT * FROM {table_name}", ttl=0)
    except Exception as e:
        st.error(f"Erro na conexão com o banco: {e}")
        return pd.DataFrame()

# =============================
# PROCESSAMENTO DO RANKING
# =============================

def processar_ranking_completo(df_ranking, col_score):
    total = len(df_ranking)
    novos_nicks, zonas = [], []
    is_bot_ranking = col_score == 'score'
    df_ranking = df_ranking.sort_values(by=col_score, ascending=is_bot_ranking).reset_index(drop=True)

    for i, row in df_ranking.iterrows():
        pos = i + 1
        nick_limpo = str(row['nick']).replace("💀", "").replace("💩", "").replace("👤", "").strip()
        if pos <= 3:
            novos_nicks.append(f"💀 {nick_limpo}"); zonas.append("Elite Zone")
        elif pos > (total - 3):
            novos_nicks.append(f"💩 {nick_limpo}"); zonas.append("Cocô Zone")
        else:
            novos_nicks.append(f"👤 {nick_limpo}"); zonas.append("Medíocre Zone")

    df_ranking['Pos'] = range(1, total + 1)
    df_ranking['nick'] = novos_nicks
    df_ranking['Classificação'] = zonas
    cols = ['Pos', 'Classificação', 'nick', 'partidas', 'kr', 'vitorias', 'kills', 'assists', 'headshots', 'revives', 'kill_dist_max', 'dano_medio']
    if col_score not in cols: cols.append(col_score)
    return df_ranking[cols]

# =============================
# INTERFACE
# =============================
st.markdown("<h1 style='text-align:left;'>🏆 PUBG Ranking Squad - Season 40</h1>", unsafe_allow_html=True)

df_bruto = get_data("v_ranking_squad_completo")
df_bots_raw = get_data("ranking_bot")

if not df_bruto.empty:
    if 'ultima_atualizacao' in df_bruto.columns:
        try:
            dt_formatada = pd.to_datetime(df_bruto['ultima_atualizacao'].iloc[0]).strftime('%d/%m/%Y %H:%M')
            st.markdown(f"<p style='text-align:left; color: #888; margin-top: -15px;'>📅 Última atualização do banco: <b>{dt_formatada}</b></p>", unsafe_allow_html=True)
        except: pass

    st.markdown("---")
    
    # Cálculos de limpeza de Bots (Mantido Original)
    cols_calc = ['partidas', 'vitorias', 'kills', 'assists', 'headshots', 'revives', 'dano_medio']
    for col in cols_calc:
        df_bruto[col] = pd.to_numeric(df_bruto[col], errors='coerce').fillna(0)
        if not df_bots_raw.empty and col in df_bots_raw.columns:
            df_bots_raw[col] = pd.to_numeric(df_bots_raw[col], errors='coerce').fillna(0)

    if not df_bots_raw.empty:
        for _, row_bot in df_bots_raw.iterrows():
            nick_bot = row_bot['nick']
            if nick_bot in df_bruto['nick'].values:
                for col in ['partidas', 'vitorias', 'kills', 'assists', 'headshots', 'revives']:
                    v_total = df_bruto.loc[df_bruto['nick'] == nick_bot, col].values[0]
                    df_bruto.loc[df_bruto['nick'] == nick_bot, col] = max(0, v_total - abs(row_bot[col]))
                p_limpas = max(1, df_bruto.loc[df_bruto['nick'] == nick_bot, 'partidas'].values[0])
                k_limpas = df_bruto.loc[df_bruto['nick'] == nick_bot, 'kills'].values[0]
                df_bruto.loc[df_bruto['nick'] == nick_bot, 'kr'] = k_limpas / p_limpas

    for col in cols_calc: df_bruto[col] = df_bruto[col].astype(int)

    def renderizar_ranking(df_local, col_score, formula, explicacao):
        if formula is not None: df_local[col_score] = formula.round(2)
        rf = processar_ranking_completo(df_local, col_score)
        
        c1, c2, c3 = st.columns(3)
        for i, col in enumerate([c1, c2, c3]):
            with col:
                n = rf.iloc[i]['nick'] if len(rf) > i else "-"
                v = f"{rf.iloc[i][col_score]:.2f} pts" if len(rf) > i else "0.00 pts"
                st.metric(f"{['🥇 1º','🥈 2º','🥉 3º'][i]} Lugar", n, v)

        st.markdown(f"<div style='background-color: #161b22; padding: 12px; border-radius: 8px; border-left: 5px solid #0078ff; margin-bottom: 20px;'>💡 {explicacao}</div>", unsafe_allow_html=True)

        fmt = {c: "{:d}" for c in ['partidas', 'vitorias', 'kills', 'assists', 'headshots', 'revives', 'dano_medio']}
        fmt.update({'kr': "{:.2f}", 'kill_dist_max': "{:.2f}", col_score: "{:.2f}"})
        
        st.dataframe(rf.style.apply(lambda r: ['background-color: #003300' if r['Classificação'] == "Elite Zone" else 'background-color: #4d0000' if r['Classificação'] == "Cocô Zone" else ''] * len(r), axis=1).format(fmt), use_container_width=True, hide_index=True)

    tab1, tab2, tab3, tab4 = st.tabs(["🔥 PRO Player", "🤝 TEAM Player", "🎯 Atirador de Elite", "🤖 Bot Detector"])
    df_v = df_bruto[df_bruto['partidas'] > 0].copy()
    df_v['p_c'] = df_v['partidas'].replace(0, 1)

    with tab1: renderizar_ranking(df_v.copy(), 'Score_Pro', (df_v['kr']*40)+(df_v['dano_medio']/8)+((df_v['vitorias']/df_v['p_c'])*500), "Fórmula PRO: Equilíbrio entre K/R, dano e vitórias.")
    with tab2: renderizar_ranking(df_v.copy(), 'Score_Team', ((df_v['vitorias']/df_v['p_c'])*1000)+((df_v['revives']/df_v['p_c'])*50)+((df_v['assists']/df_v['p_c'])*35), "Fórmula TEAM: Foco em vitórias, revives e assistências.")
    with tab3: renderizar_ranking(df_v.copy(), 'Score_Elite', (df_v['kr']*50)+((df_v['headshots']/df_v['p_c'])*60)+(df_v['dano_medio']/5), "Fórmula ELITE: Prioriza K/R e Headshots.")
    with tab4:
        if not df_bots_raw.empty: renderizar_ranking(df_bots_raw[df_bots_raw['partidas']>0].copy(), 'score', None, "Anti-Casual: Penalidade por bots.")

    st.markdown("---")
    st.markdown("<div style='text-align: center; color: gray; padding: 20px;'>📊 <b>By Adriano Vieira</b></div>", unsafe_allow_html=True)
else:
    st.warning("Conectado ao banco. Aguardando dados...")
