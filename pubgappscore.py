import streamlit as st
import pandas as pd
import requests
import psycopg2
from datetime import datetime

# =============================
# CONFIGURAÇÃO DA PÁGINA (ORIGINAL)
# =============================
st.set_page_config(
    page_title="PUBG Squad Ranking",
    layout="wide",
    page_icon="🏆",
    initial_sidebar_state="collapsed"
)

# ==========================================================
# LÓGICA DE ATUALIZAÇÃO (INVISÍVEL - NÃO AFETA LAYOUT)
# ==========================================================
@st.cache_data(ttl=300) # Roda a cada 5 minutos
def disparar_atualizacao_api():
    try:
        # Puxa credenciais das Secrets do Streamlit
        api_key = st.secrets["PUBG_API_KEY"]
        db_url = st.secrets["DATABASE_URL"]
        headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/vnd.api+json"}
        base_url = "https://api.pubg.com/shards/steam"

        # 1. Temporada atual
        res_s = requests.get(f"{base_url}/seasons", headers=headers).json()
        s_id = next(s["id"] for s in res_s["data"] if s["attributes"]["isCurrentSeason"])

        # 2. Lista de Jogadores (Mapeamento de IDs)
        nicks = ["Adrian-Wan", "MironoteuCool", "FabioEspeto", "Mamutag_Komander", "Robson_Foz", "MEIRAA", "EL-LOCORJ", "SalaminhoKBD", "nelio_ponto_dev", "CARNEIROOO", "Kowalski_PR", "Zacouteguy", "Sidors", "Takato_Matsuki", "cmm01", "Petrala", "Fumiga_BR", "O-CARRASCO"]
        
        mapping = {}
        for i in range(0, len(nicks), 10):
            g = ",".join(nicks[i:i+10])
            r = requests.get(f"{base_url}/players?filter[playerNames]={g}", headers=headers)
            if r.status_code == 200:
                for p in r.json()["data"]: mapping[p["id"]] = p["attributes"]["name"]

        # 3. Stats em Lote (Batch)
        res_stats = []
        ids = list(mapping.keys())
        for i in range(0, len(ids), 10):
            g_ids = ",".join(ids[i:i+10])
            url_batch = f"{base_url}/seasons/{s_id}/gameMode/squad/players?filter[playerIds]={g_ids}"
            rb = requests.get(url_batch, headers=headers)
            if rb.status_code == 200:
                for d in rb.json().get("data", []):
                    p_id = d["relationships"]["player"]["data"]["id"]
                    s = d["attributes"]["gameModeStats"]
                    if s.get("roundsPlayed", 0) > 0:
                        res_stats.append((mapping.get(p_id), s["roundsPlayed"], round(s["kills"]/s["roundsPlayed"], 2), s["wins"], s["kills"], int(s["damageDealt"]/s["roundsPlayed"]), s["assists"], s["headshotKills"], s["revives"], s["longestKill"], datetime.utcnow()))

        # 4. Update no Banco
        if res_stats:
            with psycopg2.connect(db_url) as conn:
                with conn.cursor() as cur:
                    sql = """INSERT INTO ranking_squad (nick, partidas, kr, vitorias, kills, dano_medio, assists, headshots, revives, kill_dist_max, atualizado_em)
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                             ON CONFLICT (nick) DO UPDATE SET partidas=EXCLUDED.partidas, kr=EXCLUDED.kr, vitorias=EXCLUDED.vitorias, kills=EXCLUDED.kills, dano_medio=EXCLUDED.dano_medio, assists=EXCLUDED.assists, headshots=EXCLUDED.headshots, revives=EXCLUDED.revives, kill_dist_max=EXCLUDED.kill_dist_max, atualizado_em=EXCLUDED.atualizado_em"""
                    cur.executemany(sql, res_stats)
                    conn.commit()
        return True
    except: return False

# Dispara a atualização silenciosamente antes de carregar o restante da página
disparar_atualizacao_api()

# =============================
# CSS TEMA ESCURO CUSTOM (ORIGINAL - RESTAURADO)
# =============================
st.markdown("""
<style>
    .stApp {
        background-color: #0e1117;
        color: white;
    }
    div[data-testid="stMetric"] {
        background-color: #161b22;
        padding: 15px;
        border-radius: 12px;
        border: 1px solid #30363d;
        text-align: center;
    }
    [data-testid="stMetricLabel"] * {
        font-size: 40px !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 38px !important;
    }
    div[data-testid="stTabs"] button {
        font-size: 16px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# =============================
# CONEXÃO COM BANCO (ORIGINAL)
# =============================
def get_data(table_name="v_ranking_squad_completo"):
    try:
        conn = st.connection(
            "postgresql",
            type="sql",
            url=st.secrets["DATABASE_URL"]
        )
        query = f"SELECT * FROM {table_name}"
        df = conn.query(query, ttl=0) 
        return df
    except Exception as e:
        st.error(f"Erro na conexão com o banco: {e}")
        return pd.DataFrame()

# =============================
# PROCESSAMENTO DO RANKING (ORIGINAL - RESTAURADO)
# =============================
def processar_ranking_completo(df_ranking, col_score):
    total = len(df_ranking)
    novos_nicks = []
    zonas = []
    
    is_bot_ranking = col_score == 'score'
    df_ranking = df_ranking.sort_values(by=col_score, ascending=is_bot_ranking).reset_index(drop=True)

    for i, row in df_ranking.iterrows():
        pos = i + 1
        nick_limpo = str(row['nick'])
        for emoji in ["💀", "💩", "👤"]:
            nick_limpo = nick_limpo.replace(emoji, "").strip()

        if pos <= 3:
            novos_nicks.append(f"💀 {nick_limpo}")
            zonas.append("Elite Zone")
        elif pos > (total - 3):
            novos_nicks.append(f"💩 {nick_limpo}")
            zonas.append("Cocô Zone")
        else:
            novos_nicks.append(f"👤 {nick_limpo}")
            zonas.append("Medíocre Zone")

    df_ranking['Pos'] = range(1, total + 1)
    df_ranking['nick'] = novos_nicks
    df_ranking['Classificação'] = zonas

    cols_base = [
        'Pos', 'Classificação', 'nick',
        'partidas', 'kr', 'vitorias',
        'kills', 'assists', 'headshots',
        'revives', 'kill_dist_max', 'dano_medio'
    ]
    
    if col_score not in cols_base:
        cols_base.append(col_score)
        
    return df_ranking[cols_base]

# ==============================================================================
# DAQUI PARA BAIXO SEGUE TODO O SEU CÓDIGO DE INTERFACE, TABS E CÁLCULOS 
# EXATAMENTE COMO VOCÊ ESCREVEU (SEM ALTERAÇÕES)
# ==============================================================================
st.markdown("<h1 style='text-align:left;'>🏆 PUBG Ranking Squad - Season 40</h1>", unsafe_allow_html=True)

df_bruto = get_data("v_ranking_squad_completo")
df_bots_raw = get_data("ranking_bot")

# ... (Continue colando o restante do seu script original aqui)
