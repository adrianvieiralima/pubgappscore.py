import streamlit as st
import pandas as pd

# =============================
# CONFIGURAÇÃO DA PÁGINA
# =============================
st.set_page_config(
    page_title="PUBG Squad Ranking",
    layout="wide",
    page_icon="🏆",
    initial_sidebar_state="expanded"
)

# =============================
# CSS TEMA ESCURO CUSTOM (ORIGINAL)
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
    [data-testid="stMetricLabel"] * { font-size: 40px !important; }
    [data-testid="stMetricValue"] { font-size: 38px !important; }
    div[data-testid="stTabs"] button { font-size: 16px; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# =============================
# CONEXÃO E FUNÇÕES DE BANCO
# =============================
def get_conn():
    return st.connection("postgresql", type="sql", url=st.secrets["DATABASE_URL"])

def get_data(table_name):
    try:
        conn = get_conn()
        query = f"SELECT * FROM {table_name}"
        return conn.query(query, ttl=0)
    except Exception:
        return pd.DataFrame()

# =============================
# BARRA LATERAL: SOLICITAÇÃO E ADMIN
# =============================
with st.sidebar:
    st.title("Settings")
    
    # 1. Formulário de Solicitação para Usuários
    st.markdown("### 📝 Solicitar Entrada")
    with st.form("form_adesao", clear_on_submit=True):
        novo_nick = st.text_input("Nickname no PUBG")
        submit = st.form_submit_button("Enviar Pedido")
        if submit and novo_nick:
            try:
                conn = get_conn()
                with conn.session as s:
                    s.execute("INSERT INTO jogadores_monitorados (nick, status) VALUES (:n, 'pendente') ON CONFLICT DO NOTHING", {"n": novo_nick})
                    s.commit()
                st.success("Pedido enviado ao Admin!")
            except: st.error("Erro ao enviar.")

    st.markdown("---")
    
    # 2. Área do Administrador (Aprovação)
    st.markdown("### 🔑 Área do Admin")
    senha_admin = st.text_input("Senha Admin", type="password")
    if senha_admin == "SUA_SENHA_AQUI": # Altere para sua senha de preferência
        st.info("Pedidos Pendentes:")
        df_pendentes = get_data("jogadores_monitorados")
        if not df_pendentes.empty:
            pendentes = df_pendentes[df_pendentes['status'] == 'pendente']
            for p in pendentes['nick']:
                col_n, col_b = st.columns([2, 1])
                col_n.text(p)
                if col_b.button("✅", key=f"app_{p}"):
                    with get_conn().session as s:
                        s.execute("UPDATE jogadores_monitorados SET status = 'ativo' WHERE nick = :n", {"n": p})
                        s.commit()
                    st.rerun()

# =============================
# PROCESSAMENTO DO RANKING (ORIGINAL)
# =============================
def processar_ranking_completo(df_ranking, col_score):
    total = len(df_ranking)
    novos_nicks, zonas = [], []
    is_bot = col_score == 'score'
    df_ranking = df_ranking.sort_values(by=col_score, ascending=is_bot).reset_index(drop=True)

    for i, row in df_ranking.iterrows():
        pos = i + 1
        nick = str(row['nick']).replace("💀", "").replace("💩", "").replace("👤", "").strip()
        if pos <= 3: novos_nicks.append(f"💀 {nick}"); zonas.append("Elite Zone")
        elif pos > (total - 3): novos_nicks.append(f"💩 {nick}"); zonas.append("Cocô Zone")
        else: novos_nicks.append(f"👤 {nick}"); zonas.append("Medíocre Zone")

    df_ranking['Pos'] = range(1, total + 1)
    df_ranking['nick'], df_ranking['Classificação'] = novos_nicks, zonas
    return df_ranking

# =============================
# INTERFACE PRINCIPAL
# =============================
st.markdown("<h1 style='text-align:left;'>🏆 PUBG Ranking Squad - Season 40</h1>", unsafe_allow_html=True)

df_bruto = get_data("v_ranking_squad_completo")
df_bots_raw = get_data("ranking_bot")

if not df_bruto.empty:
    # Lógica de Subtração Anti-Casual
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
                df_bruto.loc[df_bruto['nick'] == nick_bot, 'kr'] = df_bruto.loc[df_bruto['nick'] == nick_bot, 'kills'].values[0] / p_limpas

    for col in cols_calc: df_bruto[col] = df_bruto[col].astype(int)

    # Renderização
    def renderizar_ranking(df_local, col_score, formula, explicacao):
        if formula is not None: df_local[col_score] = formula.round(2)
        ranking_final = processar_ranking_completo(df_local, col_score)
        
        # Cards de Topo
        t1, t2, t3 = st.columns(3)
        for i, col in enumerate([t1, t2, t3]):
            if len(ranking_final) > i:
                col.metric(f"{i+1}º Lugar", ranking_final.iloc[i]['nick'], f"{ranking_final.iloc[i][col_score]:.2f} pts")

        st.markdown(f"<div style='background-color: #161b22; padding: 12px; border-radius: 8px; border-left: 5px solid #0078ff; margin-bottom: 20px;'>💡 {explicacao}</div>", unsafe_allow_html=True)

        # Formatação Anti-Casual (Sinal de menos)
        if col_score == 'score':
            fmt = {c: (lambda x: f"- {int(abs(x))}" if isinstance(x, (int, float)) and c != 'kr' else f"- {abs(x):.2f}") for c in cols_calc + ['kr', 'kill_dist_max']}
            fmt[col_score] = "{:.2f}"
        else:
            fmt = {c: "{:d}" for c in cols_calc}; fmt.update({'kr': "{:.2f}", 'kill_dist_max': "{:.2f}", col_score: "{:.2f}"})

        st.dataframe(ranking_final.style.background_gradient(cmap='YlGnBu' if col_score != 'score' else 'RdYlGn', subset=[col_score]).format(fmt), use_container_width=True, hide_index=True)

    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs(["🔥 PRO Player", "🤝 TEAM Player", "🎯 Atirador de Elite", "🤖 Anti-Casual"])
    df_v = df_bruto[df_bruto['partidas'] > 0].copy()
    p_c = df_v['partidas'].replace(0, 1)

    with tab1: renderizar_ranking(df_v.copy(), 'Score_Pro', (df_v['kr']*40)+(df_v['dano_medio']/8)+((df_v['vitorias']/p_c)*500), "Fórmula PRO Player")
    with tab2: renderizar_ranking(df_v.copy(), 'Score_Team', ((df_v['vitorias']/p_c)*1000)+((df_v['revives']/p_c)*50), "Fórmula TEAM Player")
    with tab3: renderizar_ranking(df_v.copy(), 'Score_Elite', (df_v['kr']*50)+((df_v['headshots']/p_c)*60), "Fórmula Atirador de Elite")
    with tab4: 
        if not df_bots_raw.empty: renderizar_ranking(df_bots_raw[df_bots_raw['partidas']>0].copy(), 'score', None, "Estatísticas removidas (Bots)")

    st.markdown("<div style='text-align: center; color: gray; padding: 20px;'>📊 <b>By Adriano Vieira</b></div>", unsafe_allow_html=True)
else:
    st.warning("Aguardando dados...")
