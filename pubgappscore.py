import streamlit as st
import pandas as pd
import subprocess
import sys
from datetime import datetime

# =============================
# ATUALIZAÇÃO AUTOMÁTICA DO BANCO
# =============================

if "ranking_atualizado" not in st.session_state:
    try:
        subprocess.run([sys.executable, "pubg_import.py"], check=True)
        st.session_state["ranking_atualizado"] = True
    except Exception as e:
        st.warning(f"Erro ao atualizar ranking: {e}")

# =============================
# CONFIGURAÇÃO DA PÁGINA (ORIGINAL)
# =============================

st.set_page_config(
    page_title="PUBG Squad Ranking",
    layout="wide",
    page_icon="🏆",
    initial_sidebar_state="collapsed"
)

# =============================
# CSS TEMA ESCURO CUSTOM (ORIGINAL)
# =============================

st.markdown("""
<style>
.stApp {
    background-color:#0e1117;
    color:white;
}

div[data-testid="stMetric"]{
    background-color:#161b22;
    padding:15px;
    border-radius:12px;
    border:1px solid #30363d;
    text-align:center;
}

[data-testid="stMetricLabel"] *{
    font-size:40px !important;
}

[data-testid="stMetricValue"]{
    font-size:38px !important;
}

div[data-testid="stTabs"] button{
    font-size:16px;
    font-weight:bold;
}
</style>
""", unsafe_allow_html=True)

# =============================
# CONEXÃO COM BANCO
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
# PROCESSAMENTO DO RANKING
# =============================

def processar_ranking_completo(df_ranking, col_score):
    total=len(df_ranking)
    novos_nicks=[]
    zonas=[]
    is_bot_ranking = col_score == "score"

    df_ranking = df_ranking.sort_values(
        by=col_score,
        ascending=is_bot_ranking
    ).reset_index(drop=True)

    for i,row in df_ranking.iterrows():
        pos=i+1
        nick_limpo=str(row["nick"])
        for emoji in ["💀","💩","👤"]:
            nick_limpo=nick_limpo.replace(emoji,"").strip()

        if pos<=3:
            novos_nicks.append(f"💀 {nick_limpo}")
            zonas.append("Elite Zone")
        elif pos>(total-3):
            novos_nicks.append(f"💩 {nick_limpo}")
            zonas.append("Cocô Zone")
        else:
            novos_nicks.append(f"👤 {nick_limpo}")
            zonas.append("Medíocre Zone")

    df_ranking["Pos"]=range(1,total+1)
    df_ranking["nick"]=novos_nicks
    df_ranking["Classificação"]=zonas

    cols_base=[
        "Pos","Classificação","nick",
        "partidas","kr","vitorias","top10",
        "kills","assists","headshots",
        "revives","kill_dist_max","dano_medio"
    ]
    if col_score not in cols_base:
        cols_base.append(col_score)

    return df_ranking[cols_base]

# =============================
# INTERFACE
# =============================

st.markdown(
"<h1 style='text-align:left;'>🏆 PUBG Ranking Squad - Season 40</h1>",
unsafe_allow_html=True
)

df_bruto = get_data("v_ranking_squad_completo")
df_bots_raw = get_data("ranking_bot")

if not df_bruto.empty:

    st.markdown("---")

    cols_calc=[
        "partidas","vitorias","top10","kills",
        "assists","headshots","revives","dano_medio"
    ]

    for col in cols_calc:
        if col in df_bruto.columns:
            df_bruto[col]=pd.to_numeric(df_bruto[col],errors="coerce").fillna(0)

    for col in cols_calc:
        if col in df_bruto.columns:
            df_bruto[col]=df_bruto[col].astype(int)

# =============================
# TABS
# =============================

    tab1,tab2,tab3,tab4=st.tabs([
        "🔥 PRO Player",
        "🤝 TEAM Player",
        "🎯 Atirador de Elite",
        "🤖 Bot Detector"
    ])

    df_valid=df_bruto[df_bruto["partidas"]>0].copy()
    df_valid["partidas_calc"]=df_valid["partidas"].replace(0,1)

    if "top10" not in df_valid.columns:
        df_valid["top10"]=0

# =============================
# PRO PLAYER
# =============================

    with tab1:

        f_pro = (
            (df_valid["vitorias"] / df_valid["partidas_calc"] * 5.0) +
            (df_valid["kills"] / df_valid["partidas_calc"] * 0.6) +
            (df_valid["top10"] / df_valid["partidas_calc"] * 0.5) +
            (df_valid["dano_medio"] * 0.0015) +
            (df_valid["revives"] / df_valid["partidas_calc"] * 0.4)
        )

        renderizar_ranking(
            df_valid.copy(),
            "Score_Pro",
            f_pro,
            "Fórmula PRO: Foco em eficiência geral por partida.",
            "Σ(Win:5.0, Kill:0.6, Top10:0.5, Dano:0.0015, Rev:0.4) / Partidas"
        )

# =============================
# TEAM PLAYER
# =============================

    with tab2:

        f_team = (
            (df_valid["vitorias"] / df_valid["partidas_calc"] * 7.0) +
            (df_valid["top10"] / df_valid["partidas_calc"] * 2.5) +
            (df_valid["revives"] / df_valid["partidas_calc"] * 1.2) +
            (df_valid["assists"] / df_valid["partidas_calc"] * 0.7)
        )

        renderizar_ranking(
            df_valid.copy(),
            "Score_Team",
            f_team,
            "Fórmula TEAM: foco em sobrevivência e suporte ao squad.",
            "Σ(Win:7.0, Top10:2.5, Rev:1.2, Assist:0.7) / Partidas"
        )

# =============================
# ELITE PLAYER
# =============================

    with tab3:

        f_elite = (
            (df_valid["kr"] * 2.2) +
            (df_valid["headshots"] / df_valid["partidas_calc"] * 1.5) +
            (df_valid["dano_medio"] * 0.0035) +
            (df_valid["kill_dist_max"].clip(upper=400) / 250)
        )

        renderizar_ranking(
            df_valid.copy(),
            "Score_Elite",
            f_elite,
            "Fórmula ELITE: mede precisão, dano e habilidade mecânica.",
            "Σ(KR:2.2, Headshot:1.5, Dano:0.0035, Dist(max400)/250)"
        )

else:
    st.warning("Conectado ao banco. Aguardando dados...")
