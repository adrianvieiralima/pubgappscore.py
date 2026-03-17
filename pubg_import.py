import os
import time
import requests
import psycopg2
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

DATABASE_URL = os.environ.get("DATABASE_URL")
API_KEY = os.environ.get("PUBG_API_KEY")
BASE_URL = "https://api.pubg.com/shards/steam"

headers = {"Authorization": f"Bearer {API_KEY}", "Accept": "application/vnd.api+json"}
players = ["Adrian-Wan", "MironoteuCool", "FabioEspeto", "Mamutag_Komander", "Robson_Foz", "MEIRAA", "EL-LOCORJ", "SalaminhoKBD", "nelio_ponto_dev", "CARNEIROOO", "Kowalski_PR", "Zacouteguy", "Sidors", "Takato_Matsuki", "cmm01", "Petrala", "Fumiga_BR", "O-CARRASCO"]

def fazer_requisicao(url):
    for tentativa in range(3):
        res = requests.get(url, headers=headers)
        if res.status_code == 429:
            retry_after = int(res.headers.get("Retry-After", 10))
            time.sleep(retry_after)
            continue
        return res
    return None

def dividir_lista(lista, tamanho):
    for i in range(0, len(lista), tamanho):
        yield lista[i:i + tamanho]

inicio_total = time.time()
res_season = fazer_requisicao(f"{BASE_URL}/seasons")
current_season_id = next((s["id"] for s in res_season.json()["data"] if s["attributes"]["isCurrentSeason"]), "")

player_ids = {}
for grupo in dividir_lista(players, 10):
    nomes = ",".join(grupo)
    res = fazer_requisicao(f"{BASE_URL}/players?filter[playerNames]={nomes}")
    if res and res.status_code == 200:
        for p in res.json()["data"]:
            player_ids[p["attributes"]["name"]] = p["id"]

def buscar_stats(player, p_id):
    url = f"{BASE_URL}/players/{p_id}/seasons/{current_season_id}"
    res = fazer_requisicao(url)
    if not res or res.status_code != 200: return None
    stats = res.json()["data"]["attributes"]["gameModeStats"].get("squad", {})
    partidas = stats.get("roundsPlayed", 0)
    if partidas == 0: return None
    
    return (
        player, partidas, round(stats.get("kills", 0) / partidas, 2), 
        stats.get("wins", 0), stats.get("top10s", 0), stats.get("kills", 0),
        int(stats.get("damageDealt", 0) / partidas), stats.get("assists", 0),
        stats.get("headshotKills", 0), stats.get("revives", 0),
        stats.get("longestKill", 0.0), datetime.utcnow()
    )

resultados = []
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(buscar_stats, player, p_id) for player, p_id in player_ids.items()]
    for future in as_completed(futures):
        res = future.result()
        if res: resultados.append(res)

try:
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    sql = """
    INSERT INTO ranking_squad 
    (nick, partidas, kr, vitorias, top10, kills, dano_medio, assists, headshots, revives, kill_dist_max, atualizado_em)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (nick) DO UPDATE SET
        atualizado_em = EXCLUDED.atualizado_em,
        partidas=EXCLUDED.partidas,
        kr=EXCLUDED.kr,
        vitorias=EXCLUDED.vitorias,
        top10=EXCLUDED.top10,
        kills=EXCLUDED.kills,
        dano_medio=EXCLUDED.dano_medio,
        assists=EXCLUDED.assists,
        headshots=EXCLUDED.headshots,
        revives=EXCLUDED.revives,
        kill_dist_max=EXCLUDED.kill_dist_max;
    """
    cursor.executemany(sql, resultados)
    conn.commit()
    conn.close()
    print("💾 Banco atualizado!")
except Exception as e:
    print(f"💥 Erro: {e}")
