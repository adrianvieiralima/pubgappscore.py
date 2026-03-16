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
        "partidas","kr","vitorias", "top10", # Incluído Top10 aqui
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
    if "ultima_atualizacao" in df_bruto.columns:
        try:
            dt_raw=pd.to_datetime(df_bruto["ultima_atualizacao"].iloc[0])
            dt_formatada=dt_raw.strftime("%d/%m/%Y %H:%M")
            st.markdown(
            f"<p style='text-align:left;color:#888;margin-top:-15px;'>📅 Última atualização do banco: <b>{dt_formatada}</b></p>",
            unsafe_allow_html=True
            )
        except:
            pass

    st.markdown("---")

    cols_calc=[
        "partidas","vitorias","top10","kills", # Incluído top10 no cálculo numérico
        "assists","headshots","revives","dano_medio"
    ]

    for col in cols_calc:
        if col in df_bruto.columns:
            df_bruto[col]=pd.to_numeric(df_bruto[col],errors="coerce").fillna(0)
        if not df_bots_raw.empty and col in df_bots_raw.columns:
            df_bots_raw[col]=pd.to_numeric(df_bots_raw[col],errors="coerce").fillna(0)

    if not df_bots_raw.empty:
        for _,row_bot in df_bots_raw.iterrows():
            nick_bot=row_bot["nick"]
            if nick_bot in df_bruto["nick"].values:
                for col in ["partidas","vitorias","kills","assists","headshots","revives"]:
                    v_total=df_bruto.loc[df_bruto["nick"]==nick_bot,col].values[0]
                    v_casual=abs(row_bot[col])
                    df_bruto.loc[df_bruto["nick"]==nick_bot,col]=max(0,v_total-v_casual)
                
                p_limpas=max(1,df_bruto.loc[df_bruto["nick"]==nick_bot,"partidas"].values[0])
                k_limpas=df_bruto.loc[df_bruto["nick"]==nick_bot,"kills"].values[0]
                df_bruto.loc[df_bruto["nick"]==nick_bot,"kr"]=k_limpas/p_limpas

    for col in cols_calc:
        if col in df_bruto.columns:
            df_bruto[col]=df_bruto[col].astype(int)

    def highlight_zones(row):
        if row["Classificação"]=="Elite Zone":
            return ["background-color:#003300;color:white;font-weight:bold"]*len(row)
        if row["Classificação"]=="Cocô Zone":
            return ['background-color: #5A3E1B; color: white; font-weight: bold'] * len(row)
        return [""]*len(row)

# =============================
# FUNÇÃO DE PENALIDADE POR INATIVIDADE
# =============================

    def aplicar_decaimento(df_local, col_score):
        df_local["ultima_atualizacao"] = pd.to_datetime(df_local["ultima_atualizacao"])
        hoje = pd.Timestamp.now()
        df_local["dias_inativo"] = (hoje - df_local["ultima_atualizacao"]).dt.days
        df_local["semanas_inativo"] = df_local["dias_inativo"] // 7
        df_local[col_score] = df_local[col_score] * (0.85 ** df_local["semanas_inativo"])
        return df_local

# =============================
# RENDER
# =============================

    def renderizar_ranking(df_local,col_score,formula,explicacao,calculo_discreto=""):
        if formula is not None:
            df_local[col_score]=formula.round(2)
            if col_score != "score":
                df_local = aplicar_decaimento(df_local, col_score)

        ranking_final=processar_ranking_completo(df_local,col_score)
        top1,top2,top3=st.columns(3)

        with top1:
            nome=ranking_final.iloc[0]["nick"] if len(ranking_final)>0 else "-"
            valor=f"{ranking_final.iloc[0][col_score]:.2f} pts" if len(ranking_final)>0 else "0.00 pts"
            st.metric("🥇 1º Lugar",nome,valor)
        with top2:
            nome=ranking_final.iloc[1]["nick"] if len(ranking_final)>1 else "-"
            valor=f"{ranking_final.iloc[1][col_score]:.2f} pts" if len(ranking_final)>1 else "0.00 pts"
            st.metric("🥈 2º Lugar",nome,valor)
        with top3:
            nome=ranking_final.iloc[2]["nick"] if len(ranking_final)>2 else "-"
            valor=f"{ranking_final.iloc[2][col_score]:.2f} pts" if len(ranking_final)>2 else "0.00 pts"
            st.metric("🥉 3º Lugar",nome,valor)

        st.markdown(
        f"<div style='background-color:#161b22;padding:12px;border-radius:8px;border-left:5px solid #0078ff;margin-bottom:20px;text-align:left;'>💡 {explicacao}</div>",
        unsafe_allow_html=True
        )
        if calculo_discreto:
            st.caption(f"⚙️ Cálculo: {calculo_discreto} | ⚠️ Penalidade: -15% a cada 7 dias de inatividade.")

        if col_score=="score":
            format_dict={
                "kr":lambda x:f"- {abs(x):.2f}",
                "kill_dist_max":lambda x:f"- {abs(x):.2f}",
                "partidas":lambda x:f"- {int(abs(x))}",
                "vitorias":lambda x:f"- {int(abs(x))}",
                "top10":lambda x:f"- {int(abs(x))}",
                "kills":lambda x:f"- {int(abs(x))}",
                "assists":lambda x:f"- {int(abs(x))}",
                "headshots":lambda x:f"- {int(abs(x))}",
                "revives":lambda x:f"- {int(abs(x))}",
                "dano_medio":lambda x:f"- {int(abs(x))}",
                col_score:"{:.2f}"
            }
        else:
            format_dict={
                "kr":"{:.2f}",
                "kill_dist_max":"{:.2f}",
                col_score:"{:.2f}",
                "partidas":"{:d}",
                "vitorias":"{:d}",
                "top10":"{:d}",
                "kills":"{:d}",
                "assists":"{:d}",
                "headshots":"{:d}",
                "revives":"{:d}",
                "dano_medio":"{:d}"
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
                "top10": "Top 10s",
                "kills": "Kills",
                "assists": "Assists",
                "headshots": "Headshots",
                "revives": "Revives",
                "kill_dist_max": "Kill Dist Máx",
                "dano_medio": "Dano Médio",
                "Score_Pro": "Score Pro",
                "Score_Team": "Score Team",
                "Score_Elite": "Score Elite",
                "score": "Penalidade"
            }
        )

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
    
    # Previne erro se a coluna top10 ainda não existir no dataframe
    if "top10" not in df_valid.columns:
        df_valid["top10"] = 0

    with tab1:
        f_pro=(df_valid["kr"]*40)+(df_valid["dano_medio"]/8)+((df_valid["vitorias"]/df_valid["partidas_calc"])*500)+(df_valid["top10"]*15)
        renderizar_ranking(
            df_valid.copy(),
            "Score_Pro",
            f_pro,
            "Fórmula PRO: Valoriza equilíbrio entre sobrevivência e agressividade. Foca em K/R alto, dano consistente, taxa de vitória e frequência no Top 10.",
            "(KR × 40) + (Dano Médio / 8) + (Win Rate × 500) + (Top 10s × 15)"
        )

    with tab2:
        f_team=((df_valid["vitorias"]/df_valid["partidas_calc"])*1000)+((df_valid["revives"]/df_valid["partidas_calc"])*50)+((df_valid["assists"]/df_valid["partidas_calc"])*35)+(df_valid["top10"]*30)
        renderizar_ranking(
            df_valid.copy(),
            "Score_Team",
            f_team,
            "Fórmula TEAM: Foco total no jogo coletivo. Pontua mais quem garante vitórias, sobrevive até o Top 10 e ajuda aliados.",
            "(Win Rate × 1000) + (Média Revives × 50) + (Média Assists × 35) + (Top 10s × 30)"
        )

    with tab3:
        f_elite=(df_valid["kr"]*50)+((df_valid["headshots"]/df_valid["partidas_calc"])*60)+(df_valid["dano_medio"]/5)+(df_valid["top10"]*10)
        renderizar_ranking(
            df_valid.copy(),
            "Score_Elite",
            f_elite,
            "Fórmula ELITE: Prioriza K/R, precisão de Headshots, volume de dano e consistência em chegar ao Top 10.",
            "(KR × 50) + (Média Headshots × 60) + (Dano Médio / 5) + (Top 10s × 10)"
        )

    with tab4:
        if not df_bots_raw.empty:
            df_bots=df_bots_raw[df_bots_raw["partidas"]>0].copy()
            if not df_bots.empty:
                renderizar_ranking(
                df_bots,
                "score",
                None,
                "Anti-Casual: Jogadores penalizados por matar bots em partidas no modo casual."
                )
            else:
                st.info("Nenhuma penalidade registrada.")

    # =============================
    # ESTATÍSTICAS COMPLETAS
    # =============================
    st.markdown("---")
    st.markdown("### 📊 Performance Comparativa (Top 5)")

    if not df_valid.empty:
        col_g1, col_g2 = st.columns(2)
        with col_g1:
            st.write("🔥 **Dano Médio**")
            st.bar_chart(df_valid.nlargest(5, 'dano_medio').set_index('nick')['dano_medio'], color="#ff4b4b", horizontal=True)
            st.write("🔟 **Top 10s Acumulados**") # Gráfico de Top 10 no lugar de HS
            st.bar_chart(df_valid.nlargest(5, 'top10').set_index('nick')['top10'], color="#f1c40f", horizontal=True)
        with col_g2:
            st.write("🎯 **Kills Totais**")
            st.bar_chart(df_valid.nlargest(5, 'kills').set_index('nick')['kills'], color="#f63366", horizontal=True)
            st.write("🏆 **Vitórias Totais**")
            st.bar_chart(df_valid.nlargest(5, 'vitorias').set_index('nick')['vitorias'], color="#00cc66", horizontal=True)

        st.markdown("#### 🚩 Recordes Individuais")
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
