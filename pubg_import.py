import pandas as pd
import requests
import psycopg2
from sqlalchemy import create_engine
import time

# =============================
# CONFIGURAÇÕES E CONEXÃO
# =============================
DB_URL = "SUA_DATABASE_URL_AQUI" # Use a mesma URL do seu secrets
API_KEY = "SUA_API_KEY_PUBG_AQUI"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/vnd.api+json"
}

def get_engine():
    return create_engine(DB_URL)

# ==========================================================
# FUNÇÃO PARA CARREGAR PLAYERS ATIVOS DO BANCO (DINÂMICO)
# ==========================================================
def carregar_players_ativos():
    try:
        engine = get_engine()
        query = "SELECT nick, account_id FROM jogadores_monitorados WHERE status = 'ativo'"
        df = pd.read_sql(query, engine)
        # Retorna um dicionário { 'Nick': 'AccountID' }
        return dict(zip(df['nick'], df['account_id']))
    except Exception as e:
        print(f"Erro ao carregar players do banco: {e}")
        return {}

# ==========================================================
# LÓGICA DE COLETA (ADAPTADA)
# ==========================================================
def importar_dados():
    PLAYERS = carregar_players_ativos()
    
    if not PLAYERS:
        print("Nenhum jogador ativo encontrado no banco para monitoramento.")
        return

    lista_resultados = []

    for nick, account_id in PLAYERS.items():
        print(f"Coletando dados de: {nick}...")
        
        # Se o account_id estiver vazio no banco, tentamos buscar na API e atualizar o banco
        if not account_id or account_id == "":
            url_player = f"https://api.pubg.com/shards/steam/players?filter[playerNames]={nick}"
            res = requests.get(url_player, headers=HEADERS)
            if res.status_code == 200:
                account_id = res.json()['data'][0]['id']
                # Atualiza o banco para não precisar buscar o ID de novo
                with get_engine().connect() as conn:
                    conn.execute(f"UPDATE jogadores_monitorados SET account_id = '{account_id}' WHERE nick = '{nick}'")
            else:
                print(f"Não foi possível encontrar ID para {nick}")
                continue

        # Coleta de Stats da Season (Exemplo simplificado da sua lógica atual)
        url_stats = f"https://api.pubg.com/shards/steam/players/{account_id}/seasons/division.bro.official.pc-2024-40/gameMode/squad-fpp/stats"
        res_stats = requests.get(url_stats, headers=HEADERS)
        
        if res_stats.status_code == 200:
            data = res_stats.json()['data']['attributes']['gameModeStats']['squad-fpp']
            data['nick'] = nick
            data['ultima_atualizacao'] = pd.Timestamp.now()
            lista_resultados.append(data)
        
        time.sleep(1) # Respeitar rate limit

    if lista_resultados:
        df_final = pd.DataFrame(lista_resultados)
        engine = get_engine()
        # Salva na sua view/tabela principal de ranking
        df_final.to_sql('v_ranking_squad_completo', engine, if_exists='replace', index=False)
        print("Ranking principal atualizado com sucesso!")

if __name__ == "__main__":
    importar_dados()
