import streamlit as st
import pandas as pd
import requests
import psycopg2
from datetime import datetime

# =============================
# 1. CONFIGURAÇÃO DA PÁGINA (ORIGINAL)
# =============================
st.set_page_config(
    page_title="PUBG Squad Ranking",
    layout="wide",
    page_icon="🏆",
    initial_sidebar_state="collapsed"
)

# =============================
# 2. CSS TEMA ESCURO CUSTOM (ORIGINAL)
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

# =============================
# 3. FUNÇÕES DE DADOS (ORIGINAL)
# =============================
def get_data(table_name="v_ranking_squad_completo"):
    try:
        conn = st.connection("postgresql", type="sql", url=st.secrets["DATABASE_URL"])
        return conn.query(f"SELECT * FROM {table_name}", ttl=0)
    except Exception as e:
        return pd.DataFrame()

# ==========================================================
# 4. LÓGICA DE ATUALIZAÇÃO (ISOLADA)
# ==========================================================
def realizar_update_pubg():
    """Função interna para atualizar o banco sem travar a UI"""
    try:
        api_key = st.secrets.get("PUBG_API_KEY")
        db_url = st.secrets.get("DATABASE_URL")
        if not api_key: return

        headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/vnd.api+json"}
        base_url = "https://api.pubg.com/shards/steam"

        # Busca temporada e faz o mapeamento de 10 em 10 (Conforme Ledger de correções)
        res_s = requests.get(f"{base_url}/seasons", headers=headers, timeout=5).json()
        s_id = next(s["id"] for s in res_s["data"] if s["attributes"]["isCurrentSeason"])

        players = ["Adrian-Wan", "Robson_Foz", "SalaminhoKBD", "MironoteuCool", "FabioEspeto", "Mamutag_Komander", "MEIRAA", "EL-LOCORJ", "nelio_ponto_dev", "CARNEIROOO", "Kowalski_PR", "Zacouteguy", "Sidors", "Takato_Matsuki", "cmm01", "Petrala", "Fumiga_BR", "O-CARRASCO"]
        
        mapping = {}
        for i in range(0, len(players), 10):
            g = ",".join(players[i:i+10])
            r = requests.get(f"{base_url}/players?filter[playerNames]={g}", headers=headers, timeout=5)
            if r.status_code == 200:
                for p in r.json()["data"]: mapping[p["id"]] = p["attributes"]["name"]

        res_stats = []
        ids = list(mapping.keys())
        for i in range(0, len(ids), 10):
            g_ids = ",".join(ids[i:i+10])
            url_b = f"{base_url}/seasons/{s_id}/gameMode/squad/players?filter[playerIds]={g_ids}"
            rb = requests.get(url_b, headers=headers, timeout=5)
            if rb.status_code == 200:
                for d in rb.json().get("data", []):
                    p_id = d["relationships"]["player"]["data"]["id"]
                    s = d["attributes"]["gameModeStats"]
                    if s.get("roundsPlayed", 0) > 0:
                        res_stats.append((mapping.get(p_id), s["roundsPlayed"], round(s["kills"]/s["roundsPlayed"], 2), s["wins"], s["kills"], int(s["damageDealt"]/s["roundsPlayed"]), s["assists"], s["headshotKills"], s["revives"], s["longestKill"], datetime.utcnow()))

        if res_stats:
            with psycopg2.connect(db_url) as conn:
                with conn.cursor() as cur:
                    cur.executemany("""INSERT INTO ranking_squad (nick, partidas, kr, vitorias, kills, dano_medio, assists, headshots, revives, kill_dist_max, atualizado_em)
                                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                     ON CONFLICT (nick) DO UPDATE SET partidas=EXCLUDED.partidas, kr=EXCLUDED.kr, vitorias=EXCLUDED.vitorias, kills=EXCLUDED.kills, dano_medio=EXCLUDED.dano_medio, assists=EXCLUDED.assists, headshots=EXCLUDED.headshots, revives=EXCLUDED.revives, kill_dist_max=EXCLUDED.kill_dist_max, atualizado_em=EXCLUDED.atualizado_em""", res_stats)
    except:
        pass

# =============================
# 5. INTERFACE E PROCESSAMENTO (ORIGINAL)
# =============================
# Restaure aqui sua função processar_ranking_completo, renderizar_ranking e as abas (Tabs) 
# exatamente como estavam no seu código original.

st.markdown("<h1 style='text-align:left;'>🏆 PUBG Ranking Squad - Season 40</h1>", unsafe_allow_html=True)

df_bruto = get_data("v_ranking_squad_completo")
df_bots_raw = get_data("ranking_bot")

# [AQUI CONTINUA TODO O SEU CÓDIGO DE INTERFACE ORIGINAL]

# ==========================================================
# 6. GATILHO DE ATUALIZAÇÃO (NO FINAL DO SCRIPT)
# ==========================================================
# Isso garante que a página carregue primeiro. 
# O update só acontece "depois" que o usuário já está vendo o site.
if 'ultimo_update' not in st.session_state:
    realizar_update_pubg()
    st.session_state['ultimo_update'] = datetime.now()
