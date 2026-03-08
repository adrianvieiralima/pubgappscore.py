import streamlit as st
import pandas as pd

# =============================
# IMPORT DO SCRIPT DE ATUALIZAÇÃO
# =============================
from scripts.pubg_ranking import atualizar_ranking

# Atualiza ranking quando a página abre (1 vez por sessão)
if "ranking_atualizado" not in st.session_state:
    with st.spinner("Atualizando ranking PUBG..."):
        atualizar_ranking()
    st.session_state.ranking_atualizado = True


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

    total = len(df_ranking)

    novos_nicks = []

    zonas = []

    is_bot_ranking = col_score == 'score'

    df_ranking = df_ranking.sort_values(
        by=col_score,
        ascending=is_bot_ranking
    ).reset_index(drop=True)

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

    if 'ultima_atualizacao' in df_bruto.columns:

        try:

            dt_raw = pd.to_datetime(df_bruto['ultima_atualizacao'].iloc[0])

            dt_formatada = dt_raw.strftime('%d/%m/%Y %H:%M')

            st.markdown(
                f"<p style='text-align:left; color: #888; margin-top: -15px;'>📅 Última atualização do banco: <b>{dt_formatada}</b></p>",
                unsafe_allow_html=True
            )

        except:
            pass

    st.markdown("---")

    cols_calc = [
        'partidas', 'vitorias', 'kills',
        'assists', 'headshots', 'revives', 'dano_medio'
    ]

    for col in cols_calc:

        df_bruto[col] = pd.to_numeric(
            df_bruto[col],
            errors='coerce'
        ).fillna(0)

        if not df_bots_raw.empty and col in df_bots_raw.columns:

            df_bots_raw[col] = pd.to_numeric(
                df_bots_raw[col],
                errors='coerce'
            ).fillna(0)

    if not df_bots_raw.empty:

        for _, row_bot in df_bots_raw.iterrows():

            nick_bot = row_bot['nick']

            if nick_bot in df_bruto['nick'].values:

                for col in [
                    'partidas', 'vitorias', 'kills',
                    'assists', 'headshots', 'revives'
                ]:

                    v_total = df_bruto.loc[
                        df_bruto['nick'] == nick_bot,
                        col
                    ].values[0]

                    v_casual = abs(row_bot[col])

                    df_bruto.loc[
                        df_bruto['nick'] == nick_bot,
                        col
                    ] = max(0, v_total - v_casual)

                p_limpas = max(
                    1,
                    df_bruto.loc[
                        df_bruto['nick'] == nick_bot,
                        'partidas'
                    ].values[0]
                )

                k_limpas = df_bruto.loc[
                    df_bruto['nick'] == nick_bot,
                    'kills'
                ].values[0]

                df_bruto.loc[
                    df_bruto['nick'] == nick_bot,
                    'kr'
                ] = k_limpas / p_limpas

    for col in cols_calc:

        df_bruto[col] = df_bruto[col].astype(int)

    # restante do seu código continua exatamente igual...
