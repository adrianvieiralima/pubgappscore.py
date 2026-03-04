import requests
import time
import os
import psycopg2 # Certifique-se de ter 'psycopg2-binary' no requirements.txt

# Configurações
API_KEY = os.environ.get("PUBG_API_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL") # URL do seu banco (ex: Supabase, Render, Neon)
SHARD = "steam"

PLAYERS = {
    "Adrian-Wan":"account.58beb24ada7346408942d42dc64c7901",
    "MironoteuCool":"account.24b0600cbba342eab1546ae2881f50fa",
    "FabioEspeto":"account.d8ccad228a4a417dad9921616d6c6bcd",
    "Mamutag_Komander":"account.64c62d76cce74d0b99857a27975e350e",
    "Robson_Foz":"account.8142e6d837254ee1bca954b719692f38",
    "MEIRAA":"account.c3f37890e7534978abadaf4bae051390",
    "EL-LOCORJ":"account.94ab932726fc4c64a03eb9797429baa3",
    "SalaminhoKBD":"account.de093e200d3441a9b781a9717a017dd3",
    "nelio_ponto_dev":"account.ad39c88ddf754d33a3dfeadc117c47df",
    "CARNEIROOO":"account.8c0313f2148d47b7bffcde634f094445",
    "Kowalski_PR":"account.b25200afe120424a839eb56dd2bc49cb",
    "Zacouteguy":"account.a742bf1d5725467c91140cd0ed83c265",
    "Sidors":"account.60ab21fad4094824a32dc404420b914d",
    "Takato_Matsuki":"account.10d2403139bd4066a95dda1a3eefe1e8",
    "cmm01":"account.80cedebb935242469fdd177454a52e0e",
    "Petrala":"account.aadd1c378ff841219d853b4ad2646286",
    "Fumiga_BR":"account.1fa2a7c08c3e4d4786587b4575a071cb",
    "O-CARRASCO":"account.78c6f7bd39da4274b5a3196ac624e92e",
}

def get_api(url):
    headers = {"Authorization": f"Bearer {API_KEY}", "Accept": "application/vnd.api+json"}
    r = requests.get(url, headers=headers)
    return r.json() if r.status_code == 200 else None

def processar_player(conn, name, pid):
    cur = conn.cursor()
    print(f"\n🔎 Analisando: {name}")
    
    data = get_api(f"https://api.pubg.com/shards/{SHARD}/players/{pid}")
    if not data: return
    
    matches = data["data"]["relationships"]["matches"]["data"]
    
    for m in matches[:15]: # Analisa as últimas 15 por vez
        mid = m["id"]
        
        # Verifica se já processamos essa partida antes
        cur.execute("SELECT 1 FROM matches_processadas WHERE match_id = %s", (mid,))
        if cur.fetchone(): continue
            
        m_data = get_api(f"https://api.pubg.com/shards/{SHARD}/matches/{mid}")
        if not m_data: continue
            
        attr = m_data["data"]["attributes"]
        parts = [x for x in m_data["included"] if x["type"] == "participant"]
        humanos = sum(1 for p in parts if p["attributes"]["stats"].get("playerId", "").startswith("account."))
        
        # Lógica de Detecção
        is_casual = attr.get("matchType") == "casual" or humanos <= 12
        
        if is_casual:
            # Pega stats do player nessa partida específica
            p_stats = next((x["attributes"]["stats"] for x in parts if x["attributes"]["stats"]["playerId"] == pid), None)
            
            if p_stats:
                kills = p_stats.get("kills", 0)
                dano = p_stats.get("damageDealt", 0)
                # MULTIPLICADOR NEGATIVO: Aqui a mágica acontece
                score_penalidade = (kills * 10) + (dano * 0.1)
                
                print(f"⚠️ Partida Casual {mid} detectada! Aplicando saldo negativo.")
                
                # Atualiza o ranking subtraindo os valores
                cur.execute("""
                    UPDATE ranking_bot SET 
                        kills = kills - %s,
                        score = score - %s,
                        partidas = partidas + 1,
                        atualizado_em = NOW()
                    WHERE nick = %s
                """, (kills, score_penalidade, name))
        
        # Registra que a partida já foi vista
        cur.execute("INSERT INTO matches_processadas (match_id, player_name, is_casual) VALUES (%s, %s, %s)", (mid, name, is_casual))
        conn.commit()
        time.sleep(0.5)

# Execução Principal
db_conn = psycopg2.connect(DATABASE_URL)
for name, pid in PLAYERS.items():
    processar_player(db_conn, name, pid)
db_conn.close()
