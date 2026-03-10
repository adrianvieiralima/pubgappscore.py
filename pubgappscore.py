import streamlit as st
import pandas as pd
import subprocess
import sys

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
# ATUALIZAÇÃO AUTOMÁTICA
# =============================

try:
    subprocess.run([sys.executable, "pubg_import.py"], check=True)
except Exception as e:
    st.warning(f"Erro ao atualizar ranking: {e}")

# =============================
# CSS
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
# BANCO
# =============================

def get_data(table):

    try:

        conn = st.connection(
            "postgresql",
            type="sql",
            url=st.secrets["DATABASE_URL"]
        )

        return conn.query(f"SELECT * FROM {table}", ttl=0)

    except Exception as e:

        st.error(f"Erro no banco: {e}")
        return pd.DataFrame()

# =============================
# PROCESSAMENTO RANKING
# =============================

def processar_ranking(df, col_score):

    total = len(df)

    df = df.sort_values(by=col_score, ascending=False).reset_index(drop=True)

    novos = []
    zonas = []

    for i,row in df.iterrows():

        pos = i+1

        nick = str(row["nick"])

        for emoji in ["💀","💩","👤"]:
            nick = nick.replace(emoji,"").strip()

        if pos <=3:
            novos.append(f"💀 {nick}")
            zonas.append("Elite Zone")

        elif pos > total-3:
            novos.append(f"💩 {nick}")
            zonas.append("Cocô Zone")

        else:
            novos.append(f"👤 {nick}")
            zonas.append("Medíocre Zone")

    df["Pos"] = range(1,total+1)
    df["nick"] = novos
    df["Classificação"] = zonas

    return df

# =============================
# TOP3 SEGURO
# =============================

def render_top3(df,score):

    cols = st.columns(3)

    medalhas = ["🥇 1º Lugar","🥈 2º Lugar","🥉 3º Lugar"]

    for i in range(3):

        with cols[i]:

            if i < len(df):

                nome = df.iloc[i]["nick"]
                valor = f"{df.iloc[i][score]:.2f} pts"

                st.metric(medalhas[i],nome,valor)

            else:

                st.metric(medalhas[i],"-","-")

# =============================
# INTERFACE
# =============================

st.title("🏆 PUBG Ranking Squad - Season 40")

df = get_data("v_ranking_squad_completo")
df_bot = get_data("ranking_bot")

if df.empty:

    st.warning("Banco conectado. Aguardando dados...")
    st.stop()

# =============================
# CONVERSÕES SEGURAS
# =============================

cols = [
"partidas","vitorias","kills",
"assists","headshots","revives",
"dano_medio","kr"
]

for c in cols:

    if c in df.columns:
        df[c] = pd.to_numeric(df[c],errors="coerce").fillna(0)

# =============================
# TABS
# =============================

tab1,tab2,tab3,tab4 = st.tabs([
"🔥 PRO Player",
"🤝 TEAM Player",
"🎯 Atirador de Elite",
"🤖 Bot Detector"
])

df_valid = df[df["partidas"]>0].copy()

df_valid["partidas_calc"] = df_valid["partidas"].replace(0,1)

# =============================
# PRO PLAYER
# =============================

with tab1:

    score = (
        df_valid["kr"]*40 +
        df_valid["dano_medio"]/8 +
        (df_valid["vitorias"]/df_valid["partidas_calc"])*500
    )

    df_pro = df_valid.copy()
    df_pro["Score_Pro"] = score.round(2)

    df_pro = processar_ranking(df_pro,"Score_Pro")

    render_top3(df_pro,"Score_Pro")

    st.markdown(
    """
💡 **Fórmula PRO**

Valoriza jogadores completos:

• alto K/R  
• dano consistente  
• taxa de vitória
""")

    st.dataframe(df_pro,use_container_width=True)

# =============================
# TEAM PLAYER
# =============================

with tab2:

    score = (
        (df_valid["vitorias"]/df_valid["partidas_calc"])*1000 +
        (df_valid["revives"]/df_valid["partidas_calc"])*50 +
        (df_valid["assists"]/df_valid["partidas_calc"])*35
    )

    df_team = df_valid.copy()
    df_team["Score_Team"] = score.round(2)

    df_team = processar_ranking(df_team,"Score_Team")

    render_top3(df_team,"Score_Team")

    st.markdown(
    """
💡 **Fórmula TEAM**

Ranking focado em jogo coletivo:

• vitórias  
• revives  
• assistências
""")

    st.dataframe(df_team,use_container_width=True)

# =============================
# ELITE
# =============================

with tab3:

    score = (
        df_valid["kr"]*50 +
        (df_valid["headshots"]/df_valid["partidas_calc"])*60 +
        df_valid["dano_medio"]/5
    )

    df_elite = df_valid.copy()
    df_elite["Score_Elite"] = score.round(2)

    df_elite = processar_ranking(df_elite,"Score_Elite")

    render_top3(df_elite,"Score_Elite")

    st.markdown(
    """
💡 **Fórmula ELITE**

Ranking de precisão e agressividade:

• K/R alto  
• headshots  
• dano causado
""")

    st.dataframe(df_elite,use_container_width=True)

# =============================
# BOT DETECTOR
# =============================

with tab4:

    if df_bot.empty:

        st.info("Nenhuma penalidade registrada.")

    else:

        df_bot = processar_ranking(df_bot,"score")

        render_top3(df_bot,"score")

        st.markdown(
        """
💡 **Anti-Casual**

Sistema detecta partidas:

• modo casual  
• ou com poucos jogadores humanos

Jogadores recebem **penalidade de score**.
""")

        st.dataframe(df_bot,use_container_width=True)

# =============================
# FOOTER
# =============================

st.markdown("---")

st.markdown(
"<div style='text-align:center;color:gray'>📊 Ranking PUBG • By Adriano Vieira</div>",
unsafe_allow_html=True
)
