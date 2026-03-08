import streamlit as st
import pandas as pd
import requests
import psycopg2
from datetime import datetime
import threading

# =============================
# 1. CONFIGURAÇÃO E LAYOUT (ORIGINAL)
# =============================
st.set_page_config(page_title="PUBG Squad Ranking", layout="wide", page_icon="🏆", initial_sidebar_state="collapsed")

st.markdown("""
<style>
    .stApp { background-color: #0e1117; color: white; }
    div[data-testid="stMetric"] { background-color: #161b22; padding: 15px; border-radius: 12px; border: 1px solid #30363d; text-align: center; }
</style>
""", unsafe_allow_html=True)

# =============================
# 2. FUNÇÃO DE ATUALIZAÇÃO (SILENCIOSA)
# =============================
def atualizar_dados_fundo():
    """Roda a lógica da API sem bloquear o Streamlit"""
    try:
        api_key = st.secrets["PUBG_API_KEY"]
        db_url = st.secrets["DATABASE_URL"]
        headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/vnd.api+json"}
        
        # Busca temporada e jogadores (Lote de 10)
        res_s = requests.get("https://api.pubg.com/shards/steam/seasons", headers=headers, timeout=10).json()
        s_id = next(s["id"] for s in res_s["data"] if s["attributes"]["isCurrentSeason"])
        
        nicks = ["Adrian-Wan", "Robson_Foz", "SalaminhoKBD", "MironoteuCool", "FabioEspeto", "Mamutag_Komander", "MEIRAA", "EL-LOCORJ", "nelio_ponto_dev", "CARNEIROOO", "Kowalski_PR", "Zacouteguy", "Sidors", "Takato_Matsuki", "cmm01", "Petrala", "Fumiga_BR", "O-CARRASCO"]
        
        mapping = {}
        for i in range(0, len(nicks), 10):
            g = ",".join(nicks[i:i+10])
            r = requests.get(f"https://api.pubg.com/shards/steam/players?filter[playerNames]={g}", headers=headers, timeout=10)
            if r.status_code == 200:
                for p in r.json()["data"]: mapping[p["id"]] = p["attributes"]["name"]

        res_stats = []
        ids = list(mapping.keys())
        for i in range(0, len(ids), 10):
            g_ids = ",".join(ids[i:i+10])
            rb = requests.get(f"https://api.pubg.com/shards/steam/seasons/{s_id}/gameMode/squad/players?filter[playerIds]={g_ids}", headers=headers, timeout=10)
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
    except: pass

# =============================
# 3. GATILHO DE EXECUÇÃO
# =============================
st.markdown("<h1 style='text-align:left;'>🏆 PUBG Ranking Squad - Season 40</h1>", unsafe_allow_html=True)

# Dispara a atualização em uma thread separada (não trava a página)
if 'thread_rodando' not in st.session_state:
    thread = threading.Thread(target=atualizar_dados_fundo)
    thread.start()
    st.session_state['thread_rodando'] = True

# Carrega o que já tem no banco imediatamente
try:
    conn = st.connection("postgresql", type="sql", url=st.secrets["DATABASE_URL"])
    df_bruto = conn.query("SELECT * FROM v_ranking_squad_completo", ttl=0)
    df_bots_raw = conn.query("SELECT * FROM ranking_bot", ttl=0)
except:
    df_bruto, df_bots_raw = pd.DataFrame(), pd.DataFrame()

# =============================
# 4. RESTANTE DO SEU CÓDIGO (PROCESSAR_RANKING, TABS, ETC)
# =============================
# Mantenha aqui todo o seu código original de exibição...
