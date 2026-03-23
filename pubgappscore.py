# pubgappscore.py
import streamlit as st
import pandas as pd
import subprocess
import sys
import plotly.express as px
from datetime import datetime
MESES_PT = {
    1: "janeiro", 2: "fevereiro", 3: "março", 4: "abril",
    5: "maio", 6: "junho", 7: "julho", 8: "agosto",
    9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro"
}

if "ranking_atualizado" not in st.session_state:
    try:
        subprocess.run([sys.executable, "pubg_import.py"], check=True)
        st.session_state["ranking_atualizado"] = True
    except Exception as e:
        st.warning(f"Erro ao atualizar ranking: {e}")

st.set_page_config(
    page_title="PUBG Squad Ranking",
    layout="wide",
    page_icon="🏆",
    initial_sidebar_state="collapsed"
)

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

def get_data_semanal():
    try:
        conn = st.connection(
            "postgresql",
            type="sql",
            url=st.secrets["DATABASE_URL"]
        )
        df = conn.query("SELECT * FROM ranking_semanal ORDER BY semana DESC", ttl=0)
        return df
    except Exception as e:
        st.error(f"Erro ao buscar dados semanais: {e}")
        return pd.DataFrame()

def processar_ranking_completo(df_ranking, col_score):
    total = len(df_ranking)
    novos_nicks = []
    zonas = []
    is_bot_ranking = col_score == "score"

    df_ranking = df_ranking.sort_values(
        by=col_score,
        ascending=is_bot_ranking
    ).reset_index(drop=True)

    for i, row in df_ranking.iterrows():
        pos = i + 1
        nick_limpo = str(row["nick"])
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

    df_ranking["Pos"] = range(1, total + 1)
    df_ranking["nick"] = novos_nicks
    df_ranking["Classificação"] = zonas

    cols_base = [
        "Pos", "Classificação", "nick",
        "partidas", "kr", "vitorias",
        "kills", "assists", "headshots",
        "revives", "kill_dist_max", "dano_medio", "top10"
    ]
    if col_score not in cols_base:
        cols_base.append(col_score)

    return df_ranking[cols_base]

def grafico_horizontal(df, col, titulo, cor):
    df_sorted = df.sort_values(col, ascending=True).copy()
    fig = px.bar(
        df_sorted,
        x=col,
        y="nick",
        orientation="h",
        title=titulo,
        color_discrete_sequence=[cor]
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="white",
        title_font_color="white",
        xaxis=dict(showgrid=True, gridcolor="#2a2a2a"),
        yaxis=dict(showgrid=False),
        margin=dict(l=10, r=10, t=40, b=10),
        height=500
    )
    st.plotly_chart(fig, use_container_width=True)

st.markdown(
    "<h1 style='text-align:left;'>🏆 PUBG Ranking Squad - Season 40</h1>",
    unsafe_allow_html=True
)

df_bruto = get_data("v_ranking_squad_completo")
df_bots_raw = get_data("ranking_bot")
df_semanal = get_data_semanal()

if not df_bruto.empty:
    if "ultima_atualizacao" in df_bruto.columns:
        try:
            dt_raw = pd.to_datetime(df_bruto["ultima_atualizacao"].iloc[0])
            dt_formatada = dt_raw.strftime("%d/%m/%Y %H:%M")
            st.markdown(
                f"<p style='text-align:left;color:#888;margin-top:-15px;'>📅 Última atualização do banco: <b>{dt_formatada}</b></p>",
                unsafe_allow_html=True
            )
        except:
            pass

    st.markdown("---")

    cols_calc = [
        "partidas", "vitorias", "kills",
        "assists", "headshots", "revives", "dano_medio", "top10"
    ]

    for col in cols_calc:
        df_bruto[col] = pd.to_numeric(df_bruto[col], errors="coerce").fillna(0)
        if not df_bots_raw.empty and col in df_bots_raw.columns:
            df_bots_raw[col] = pd.to_numeric(df_bots_raw[col], errors="coerce").fillna(0)

    if not df_bots_raw.empty:
        for _, row_bot in df_bots_raw.iterrows():
            nick_bot = row_bot["nick"]
            if nick_bot in df_bruto["nick"].values:
                for col in ["partidas", "vitorias", "kills", "assists", "headshots", "revives", "top10"]:
                    v_total = df_bruto.loc[df_bruto["nick"] == nick_bot, col].values[0]
                    v_casual = abs(row_bot[col])
                    df_bruto.loc[df_bruto["nick"] == nick_bot, col] = max(0, v_total - v_casual)

                p_limpas = max(1, df_bruto.loc[df_bruto["nick"] == nick_bot, "partidas"].values[0])
                k_limpas = df_bruto.loc[df_bruto["nick"] == nick_bot, "kills"].values[0]
                df_bruto.loc[df_bruto["nick"] == nick_bot, "kr"] = k_limpas / p_limpas

    for col in cols_calc:
        df_bruto[col] = df_bruto[col].astype(int)

    def highlight_zones(row):
        if row["Classificação"] == "Elite Zone":
            return ["background-color:#003300;color:white;font-weight:bold"] * len(row)
        if row["Classificação"] == "Cocô Zone":
            return ['background-color: #5A3E1B; color: white; font-weight: bold'] * len(row)
        return [""] * len(row)

    def aplicar_decaimento(df_local, col_score):
        hoje = pd.Timestamp.utcnow()
        df_local["updated_at"] = pd.to_datetime(df_local["updated_at"], utc=True, errors="coerce")
        df_local["atualizado_em"] = pd.to_datetime(df_local["atualizado_em"], utc=True, errors="coerce")
        df_local["data_referencia"] = df_local["updated_at"].fillna(df_local["atualizado_em"])
        df_local["dias_inativo"] = (hoje - df_local["data_referencia"]).dt.days.fillna(0)
        df_local["semanas_inativo"] = (df_local["dias_inativo"] // 7).astype(int)
        df_local[col_score] = df_local[col_score] * (0.85 ** df_local["semanas_inativo"])
        return df_local

    def renderizar_ranking(df_local, col_score, formula, explicacao, calculo_discreto=""):
        if formula is not None:
            df_local[col_score] = formula.round(2)
            if col_score != "score":
                df_local = aplicar_decaimento(df_local, col_score)

        ranking_final = processar_ranking_completo(df_local, col_score)
        top1, top2, top3 = st.columns(3)

        with top1:
            nome = ranking_final.iloc[0]["nick"] if len(ranking_final) > 0 else "-"
            valor = f"{ranking_final.iloc[0][col_score]:.2f} pts" if len(ranking_final) > 0 else "0.00 pts"
            st.metric("🥇 1º Lugar", nome, valor)
        with top2:
            nome = ranking_final.iloc[1]["nick"] if len(ranking_final) > 1 else "-"
            valor = f"{ranking_final.iloc[1][col_score]:.2f} pts" if len(ranking_final) > 1 else "0.00 pts"
            st.metric("🥈 2º Lugar", nome, valor)
        with top3:
            nome = ranking_final.iloc[2]["nick"] if len(ranking_final) > 2 else "-"
            valor = f"{ranking_final.iloc[2][col_score]:.2f} pts" if len(ranking_final) > 2 else "0.00 pts"
            st.metric("🥉 3º Lugar", nome, valor)

        st.markdown(
            f"<div style='background-color:#161b22;padding:12px;border-radius:8px;border-left:5px solid #0078ff;margin-bottom:20px;text-align:left;'>💡 {explicacao}</div>",
            unsafe_allow_html=True
        )
        if calculo_discreto:
            st.caption(f"⚙️ Cálculo: {calculo_discreto} | ⚠️ Penalidade: -15% a cada 7 dias de inatividade.")

        if col_score == "score":
            format_dict = {
                "kr": lambda x: f"- {abs(x):.2f}",
                "kill_dist_max": lambda x: f"- {abs(x):.2f}",
                "partidas": lambda x: f"- {int(abs(x))}",
                "vitorias": lambda x: f"- {int(abs(x))}",
                "kills": lambda x: f"- {int(abs(x))}",
                "assists": lambda x: f"- {int(abs(x))}",
                "headshots": lambda x: f"- {int(abs(x))}",
                "revives": lambda x: f"- {int(abs(x))}",
                "dano_medio": lambda x: f"- {int(abs(x))}",
                "top10": lambda x: f"- {int(abs(x))}",
                col_score: "{:.2f}"
            }
        else:
            format_dict = {
                "kr": "{:.2f}",
                "kill_dist_max": "{:.2f}",
                col_score: "{:.2f}",
                "partidas": "{:d}",
                "vitorias": "{:d}",
                "kills": "{:d}",
                "assists": "{:d}",
                "headshots": "{:d}",
                "revives": "{:d}",
                "dano_medio": "{:d}",
                "top10": "{:d}"
            }

        st.dataframe(
            ranking_final.style
            .background_gradient(cmap='YlGnBu' if col_score != 'score' else 'RdYlGn', subset=[col_score])
            .apply(highlight_zones, axis=1)
            .format(format_dict),
            use_container_width=True,
            height=(len(ranking_final) * 35) + 80,
            hide_index=True,
            column_config={
                "nick": "Nickname",
                "partidas": "Partidas",
                "kr": "K/R",
                "vitorias": "Vitórias",
                "kills": "Kills",
                "assists": "Assists",
                "headshots": "Headshots",
                "revives": "Revives",
                "kill_dist_max": "Kill Dist Máx",
                "dano_medio": "Dano Médio",
                "top10": "Top 10",
                "Score_Pro": "Score Pro",
                "Score_Team": "Score Team",
                "Score_Elite": "Score Elite",
                "score": "Penalidade"
            }
        )

    tab1, tab2, tab3, tab4 = st.tabs([
        "🔥 PRO Player",
        "🤝 TEAM Player",
        "🎯 Atirador de Elite",
        "🤖 Bot Detector"
    ])

    df_valid = df_bruto[df_bruto["partidas"] > 0].copy()
    df_valid["partidas_calc"] = df_valid["partidas"].replace(0, 1)

    with tab1:
        f_pro = (
            (df_valid["vitorias"] / df_valid["partidas_calc"] * 5) +
            (df_valid["kills"] / df_valid["partidas_calc"] * 0.5) +
            (df_valid["assists"] / df_valid["partidas_calc"] * 0.1) +
            (df_valid["headshots"] / df_valid["partidas_calc"] * 0.2) +
            (df_valid["revives"] / df_valid["partidas_calc"] * 0.33) +
            (df_valid["dano_medio"] * 0.001)
        )
        renderizar_ranking(
            df_valid.copy(),
            "Score_Pro",
            f_pro,
            "Fórmula PRO: Equilíbrio entre sobrevivência e agressividade. Valoriza consistência em vitórias, kills, precisão, suporte e dano.",
            "(Win Rate × 5) + (Kills/P × 0.5) + (Assists/P × 0.1) + (Headshots/P × 0.2) + (Revives/P × 0.33) + (Dano Médio × 0.001)"
        )

    with tab2:
        f_team = (
            (df_valid["vitorias"] / df_valid["partidas_calc"] * 8) +
            (df_valid["revives"] / df_valid["partidas_calc"] * 5) +
            (df_valid["assists"] / df_valid["partidas_calc"] * 3) +
            (df_valid["top10"] / df_valid["partidas_calc"] * 2) +
            (df_valid["dano_medio"] * 0.001)
        )
        renderizar_ranking(
            df_valid.copy(),
            "Score_Team",
            f_team,
            "Fórmula TEAM: Foco total em suporte e sobrevivência coletiva. Valoriza vitórias, revives, assists e top10 por partida.",
            "(Win Rate × 8) + (Revives/P × 5) + (Assists/P × 3) + (Top10/P × 2) + (Dano Médio × 0.001)"
        )

    with tab3:
        f_elite = (
            (df_valid["kr"] * 15) +
            (df_valid["headshots"] / df_valid["partidas_calc"] * 3) +
            (df_valid["kill_dist_max"] * 0.05) +
            (df_valid["dano_medio"] * 0.003) +
            (df_valid["kills"] / df_valid["partidas_calc"] * 0.5)
        )
        renderizar_ranking(
            df_valid.copy(),
            "Score_Elite",
            f_elite,
            "Fórmula ELITE: Prioriza KR, precisão de headshots por partida, alcance máximo e volume de dano.",
            "(KR × 15) + (Headshots/P × 3) + (Kill Dist Máx × 0.05) + (Dano Médio × 0.003) + (Kills/P × 0.5)"
        )

    with tab4:
        if not df_bots_raw.empty:
            df_bots = df_bots_raw[df_bots_raw["partidas"] > 0].copy()
            if not df_bots.empty:
                renderizar_ranking(
                    df_bots,
                    "score",
                    None,
                    "Anti-Casual: Jogadores penalizados por matar bots em partidas no modo casual."
                )
            else:
                st.info("Nenhuma penalidade registrada.")

    # ===============================
    # PERFORMANCE COMPARATIVA
    # ===============================
    st.markdown("---")
    st.markdown("### 📊 Performance Comparativa")

    opcao_periodo = st.radio(
        "Selecione o período:",
        options=["📅 Por Semana", "🏆 Temporada Completa"],
        horizontal=True
    )

    if opcao_periodo == "📅 Por Semana":
        if not df_semanal.empty:
            df_semanal["semana"] = pd.to_datetime(df_semanal["semana"])
            semanas_disponiveis = sorted(df_semanal["semana"].unique(), reverse=True)

            def formatar_semana(s):
                inicio = pd.Timestamp(s)
                fim = inicio + pd.Timedelta(days=6)
                # Se a semana cruza para outro mês, usa o fim como referência
                if fim.month != inicio.month:
                    ref = fim
                else:
                    ref = inicio
                num_semana = ((ref.day - 1) // 7) + 1
                mes = f"{MESES_PT[fim.month]} {fim.year}"
                return f"Semana #{num_semana} - {mes.capitalize()}"

            semanas_labels = {s: formatar_semana(s) for s in semanas_disponiveis}

            semana_selecionada = st.selectbox(
                "Selecione a semana:",
                options=semanas_disponiveis,
                format_func=lambda s: semanas_labels[s]
            )

            df_semana_atual = df_semanal[df_semanal["semana"] == semana_selecionada].copy()

            idx_semana = list(semanas_disponiveis).index(semana_selecionada)
            if idx_semana + 1 < len(semanas_disponiveis):
                semana_anterior = semanas_disponiveis[idx_semana + 1]
                df_semana_anterior = df_semanal[df_semanal["semana"] == semana_anterior].copy()
                df_semana_anterior = df_semana_anterior.set_index("nick")

                def calcular_diff(row, col):
                    nick = row["nick"]
                    if nick in df_semana_anterior.index:
                        return row[col] - df_semana_anterior.loc[nick, col]
                    return row[col]

                for col in ["partidas", "vitorias", "kills", "assists", "headshots", "revives", "dano_medio", "top10"]:
                    df_semana_atual[col] = df_semana_atual.apply(lambda r: calcular_diff(r, col), axis=1)
            else:
                st.caption("📊 Estatísticas da Semana")

            df_graf = df_semana_atual

        else:
            st.info("Nenhum dado semanal disponível ainda. Aguarde o próximo sync.")
            df_graf = None

    else:
        df_graf = df_valid.copy()

    if df_graf is not None and not df_graf.empty:
        col_g1, col_g2 = st.columns(2)
        with col_g1:
            grafico_horizontal(df_graf, "dano_medio", "🔥 Dano Médio", "#ff4b4b")
            grafico_horizontal(df_graf, "headshots", "💀 Headshots", "#0078ff")
        with col_g2:
            grafico_horizontal(df_graf, "kills", "🎯 Kills", "#f63366")
            grafico_horizontal(df_graf, "vitorias", "🏆 Vitórias", "#00cc66")

    st.markdown("#### 🚩 Recordes Individuais")
    if not df_valid.empty:
        r1, r2, r3, r4 = st.columns(4)
        with r1:
            top = df_valid.loc[df_valid['kill_dist_max'].idxmax()]
            st.info(f"**Sniper de Elite**\n\n{top['nick']}\n\n**{top['kill_dist_max']:.1f}m**")
        with r2:
            top = df_valid.loc[df_valid['revives'].idxmax()]
            st.success(f"**Anjo da Guarda**\n\n{top['nick']}\n\n**{int(top['revives'])}** Revives")
        with r3:
            top = df_valid.loc[df_valid['assists'].idxmax()]
            st.warning(f"**Braço Direito**\n\n{top['nick']}\n\n**{int(top['assists'])}** Assists")
        with r4:
            top = df_valid.loc[df_valid['partidas'].idxmax()]
            st.error(f"**Viciado no Drop**\n\n{top['nick']}\n\n**{int(top['partidas'])}** Partidas")

    st.markdown("---")
    st.markdown(
        "<div style='text-align:center;color:gray;padding:20px;'>📊 <b>By Adriano Vieira</b></div>",
        unsafe_allow_html=True
    )

else:
    st.warning("Conectado ao banco. Aguardando dados...")
