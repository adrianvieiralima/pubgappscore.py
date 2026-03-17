import streamlit as st
import pandas as pd
import subprocess
import sys
from datetime import datetime

if "ranking_atualizado" not in st.session_state:
    try:
        subprocess.run([sys.executable, "pubg_import.py"], check=True)
        st.session_state["ranking_atualizado"] = True
    except Exception as e:
        st.warning(f"Erro ao atualizar ranking: {e}")

st.set_page_config(page_title="PUBG Squad Ranking", layout="wide", page_icon="🏆", initial_sidebar_state="collapsed")

st.markdown("""<style>.stApp { background-color:#0e1117; color:white; } div[data-testid="stMetric"]{ background-color:#161b22; padding:15px; border-radius:12px; border:1px solid #30363d; text-align:center; } [data-testid="stMetricLabel"] *{ font-size:40px !important; } [data-testid="stMetricValue"]{ font-size:38px !important; } div[data-testid="stTabs"] button{ font-size:16px; font-weight:bold; }</style>""", unsafe_allow_html=True)

def get_data(table_name="v_ranking_squad_completo"):
    try:
        conn = st.connection("postgresql", type="sql", url=st.secrets["DATABASE_URL"])
        return conn.query(f"SELECT * FROM {table_name}", ttl=0)
    except:
        return pd.DataFrame()

def processar_ranking_completo(df_ranking, col_score):
    total = len(df_ranking)
    novos_nicks, zonas = [], []
    is_bot_ranking = col_score == "score"
    df_ranking = df_ranking.sort_values(by=col_score, ascending=is_bot_ranking).reset_index(drop=True)

    for i, row in df_ranking.iterrows():
        pos = i + 1
        nick_limpo = str(row["nick"]).replace("💀","").replace("💩","").replace("👤","").strip()
        if pos <= 3:
            novos_nicks.append(f"💀 {nick_limpo}"); zonas.append("Elite Zone")
        elif pos > (total - 3):
            novos_nicks.append(f"💩 {nick_limpo}"); zonas.append("Cocô Zone")
        else:
            novos_nicks.append(f"👤 {nick_limpo}"); zonas.append("Medíocre Zone")

    df_ranking["Pos"], df_ranking["nick"], df_ranking["Classificação"] = range(1, total + 1), novos_nicks, zonas
    cols_base = ["Pos","Classificação","nick","partidas","kr","vitorias", "top10","kills","assists","headshots","revives","kill_dist_max","dano_medio"]
    if col_score not in cols_base: cols_base.append(col_score)
    return df_ranking[cols_base]

st.markdown("<h1 style='text-align:left;'>🏆 PUBG Ranking Squad - Season 40</h1>", unsafe_allow_html=True)

df_bruto = get_data("v_ranking_squad_completo")
df_bots_raw = get_data("ranking_bot")

if not df_bruto.empty:
    if "ultima_atualizacao" in df_bruto.columns:
        try:
            dt_formatada = pd.to_datetime(df_bruto["ultima_atualizacao"].iloc[0]).strftime("%d/%m/%Y %H:%M")
            st.markdown(f"<p style='text-align:left;color:#888;margin-top:-15px;'>📅 Última atualização: <b>{dt_formatada}</b></p>", unsafe_allow_html=True)
        except: pass

    st.markdown("---")
    cols_calc = ["partidas","vitorias","top10","kills","assists","headshots","revives","dano_medio"]
    for col in cols_calc:
        if col in df_bruto.columns: df_bruto[col] = pd.to_numeric(df_bruto[col], errors="coerce").fillna(0)
        if not df_bots_raw.empty and col in df_bots_raw.columns: df_bots_raw[col] = pd.to_numeric(df_bots_raw[col], errors="coerce").fillna(0)

    if not df_bots_raw.empty:
        for _, row_bot in df_bots_raw.iterrows():
            nick_bot = row_bot["nick"]
            if nick_bot in df_bruto["nick"].values:
                for col in ["partidas","vitorias","kills","assists","headshots","revives", "top10"]:
                    v_total = df_bruto.loc[df_bruto["nick"]==nick_bot, col].values[0]
                    v_casual = abs(row_bot[col])
                    df_bruto.loc[df_bruto["nick"]==nick_bot, col] = max(0, v_total - v_casual)
                p_limpas = max(1, df_bruto.loc[df_bruto["nick"]==nick_bot, "partidas"].values[0])
                df_bruto.loc[df_bruto["nick"]==nick_bot, "kr"] = df_bruto.loc[df_bruto["nick"]==nick_bot, "kills"].values[0] / p_limpas

    def aplicar_decaimento(df_local, col_score):
        df_local["ultima_atualizacao"] = pd.to_datetime(df_local["ultima_atualizacao"])
        df_local[col_score] = df_local[col_score] * (0.85 ** ((pd.Timestamp.now() - df_local["ultima_atualizacao"]).dt.days // 7))
        return df_local

    def renderizar_ranking(df_local, col_score, formula, explicacao, calculo_discreto=""):
        if formula is not None:
            df_local[col_score] = formula.round(2)
            if col_score != "score": df_local = aplicar_decaimento(df_local, col_score)
        
        ranking_final = processar_ranking_completo(df_local, col_score)
        t1, t2, t3 = st.columns(3)
        for i, (col, icon) in enumerate(zip([t1, t2, t3], ["🥇 1º", "🥈 2º", "🥉 3º"])):
            with col: st.metric(f"{icon} Lugar", ranking_final.iloc[i]["nick"] if len(ranking_final) > i else "-", f"{ranking_final.iloc[i][col_score]:.2f} pts" if len(ranking_final) > i else "0.00 pts")

        st.markdown(f"<div style='background-color:#161b22;padding:12px;border-radius:8px;border-left:5px solid #0078ff;margin-bottom:20px;'>💡 {explicacao}</div>", unsafe_allow_html=True)
        
        st.dataframe(ranking_final.style.background_gradient(cmap='YlGnBu' if col_score != 'score' else 'RdYlGn', subset=[col_score]).format(precision=2), use_container_width=True, hide_index=True)

    tab1, tab2, tab3, tab4 = st.tabs(["🔥 PRO Player", "🤝 TEAM Player", "🎯 Atirador de Elite", "🤖 Bot Detector"])
    df_valid = df_bruto[df_bruto["partidas"] > 0].copy()
    df_valid["partidas_calc"] = df_valid["partidas"].replace(0, 1)

    with tab1:
        f_pro = ((df_valid["vitorias"]/df_valid["partidas_calc"]*5) + (df_valid["kills"]/df_valid["partidas_calc"]*0.5) + (df_valid["top10"]/df_valid["partidas_calc"]*0.5) + (df_valid["assists"]/df_valid["partidas_calc"]*0.2) + (df_valid["headshots"]/df_valid["partidas_calc"]*0.2) + (df_valid["dano_medio"]*0.002) + (df_valid["revives"]/df_valid["partidas_calc"]*0.33))
        renderizar_ranking(df_valid.copy(), "Score_Pro", f_pro, "Fórmula PRO: Foco em Eficiência por Partida.")

    with tab2:
        f_team = ((df_valid["vitorias"]/df_valid["partidas_calc"]*7) + (df_valid["top10"]/df_valid["partidas_calc"]*2.5) + (df_valid["revives"]/df_valid["partidas_calc"]*1) + (df_valid["assists"]/df_valid["partidas_calc"]*0.5) + (df_valid["headshots"]/df_valid["partidas_calc"]*0.1) + (df_valid["dano_medio"]*0.001))
        renderizar_ranking(df_valid.copy(), "Score_Team", f_team, "Fórmula TEAM: Foco em Sobrevivência e Suporte.")

    with tab3:
        f_elite = ((df_valid["kr"]*2) + (df_valid["headshots"]/df_valid["partidas_calc"]*1.5) + (df_valid["dano_medio"]*0.005) + (df_valid["kill_dist_max"]/100) + (df_valid["assists"]/df_valid["partidas_calc"]*0.4))
        renderizar_ranking(df_valid.copy(), "Score_Elite", f_elite, "Fórmula ELITE: Foco em Letalidade Técnica.")

    with tab4:
        if not df_bots_raw.empty:
            renderizar_ranking(df_bots_raw[df_bots_raw["partidas"]>0].copy(), "score", None, "Anti-Casual: Penalidades por bots.")

    st.markdown("---")
    st.markdown("<div style='text-align:center;color:gray;padding:20px;'>📊 <b>By Adriano Vieira</b></div>", unsafe_allow_html=True)
