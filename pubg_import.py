# pubg_import.py
import os
import time
import requests
import psycopg2
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

DATABASE_URL = os.environ.get("DATABASE_URL")
API_KEY = os.environ.get("PUBG_API_KEY")
BASE_URL = "https://api.pubg.com/shards/steam"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/vnd.api+json"
}

players = [
    "Adrian-Wan", "MironoteuCool", "FabioEspeto", "Mamutag_Komander",
    "Robson_Foz", "MEIRAA", "EL-LOCORJ", "SalaminhoKBD",
    "nelio_ponto_dev", "CARNEIROOO", "Kowalski_PR", "Zacouteguy",
    "Sidors", "Takato_Matsuki", "cmm01", "Petrala",
    "Fumiga_BR", "O-CARRASCO"
]

def fazer_requisicao(url):
    for tentativa in range(3):
        res = requests.get(url, headers=headers)
        if res.status_code == 429:
            retry_after = int(res.headers.get("Retry-After", 10))
            print(f"⏳ Rate limit. Aguardando {retry_after}s...")
            time.sleep(retry_after)
            continue
        return res
    return None

def dividir_lista(lista, tamanho):
    for i in range(0, len(lista), tamanho):
        yield lista[i:i + tamanho]

def get_segunda_feira():
    hoje = date.today()
    return hoje - timedelta(days=hoje.weekday())

inicio_total = time.time()
print("🚀 Detectando temporada...")

# ======== AJUSTE AQUI (somente isso foi alterado) =========
res_season = fazer_requisicao(f"{BASE_URL}/seasons")
seasons = res_season.json()["data"]

current_season = next(
    (s for s in seasons if s["attributes"]["isCurrentSeason"]),
    None
)

current_season_id = current_season["id"] if current_season else ""
# ==========================================================

print(f"📅 Temporada atual: {current_season_id}")

print("🔎 Buscando IDs e última partida em lote...")
player_ids = {}
player_last_match = {}

for grupo in dividir_lista(players, 10):
    nomes = ",".join(grupo)
    res = fazer_requisicao(
        f"{BASE_URL}/players?filter[playerNames]={nomes}"
    )
    if res and res.status_code == 200:
        for p in res.json()["data"]:
            nick = p["attributes"]["name"]
            player_ids[nick] = p["id"]
            matches = p["relationships"]["matches"]["data"]
            if matches:
                player_last_match[nick] = matches[0]["id"]

print(f"✅ {len(player_ids)} IDs encontrados.")

print("📅 Buscando data da última partida...")
player_updated_at = {}

def buscar_data_partida(nick, match_id):
    res = fazer_requisicao(f"{BASE_URL}/matches/{match_id}")
    if not res or res.status_code != 200:
        return nick, None
    try:
        created_at = res.json()["data"]["attributes"]["createdAt"]
        return nick, datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return nick, None

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [
        executor.submit(buscar_data_partida, nick, match_id)
        for nick, match_id in player_last_match.items()
    ]
    for future in as_completed(futures):
        nick, data = future.result()
        player_updated_at[nick] = data
        print(f"📅 {nick} | última partida: {data}")

def buscar_stats(player, p_id):
    url = f"{BASE_URL}/players/{p_id}/seasons/{current_season_id}"
    res = fazer_requisicao(url)

    if not res or res.status_code != 200:
        return None

    stats = res.json()["data"]["attributes"]["gameModeStats"].get("squad", {})
    partidas = stats.get("roundsPlayed", 0)

    ultima_partida = player_updated_at.get(player, None)

    if partidas == 0:
        if ultima_partida:
            print(f"⚠️ {player} sem partidas na API — atualizando apenas updated_at: {ultima_partida}")
            return ("only_date", player, ultima_partida)
        print(f"⚠️ {player} sem partidas na API e sem data disponível — ignorando")
        return None

    kills = stats.get("kills", 0)
    vitorias = stats.get("wins", 0)
    assists = stats.get("assists", 0)
    headshots = stats.get("headshotKills", 0)
    revives = stats.get("revives", 0)
    dano_total = stats.get("damageDealt", 0)
    dist_max = stats.get("longestKill", 0.0)
    top10 = stats.get("top10s", 0)

    kr = round(kills / partidas, 2)
    dano_medio = int(dano_total / partidas)

    print(f"⚡ {player} processado")

    return (
        player, partidas, kr, vitorias, kills,
        dano_medio, assists, headshots,
        revives, dist_max, top10, datetime.utcnow(), ultima_partida
    )

print("⚡ Buscando estatísticas em paralelo...")

resultados = []
only_date_updates = []

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [
        executor.submit(buscar_stats, player, p_id)
        for player, p_id in player_ids.items()
    ]

    for future in as_completed(futures):
        resultado = future.result()
        if resultado is None:
            continue
        if resultado[0] == "only_date":
            only_date_updates.append((resultado[2], resultado[1]))
        else:
            resultados.append(resultado)

print(f"✅ {len(resultados)} jogadores com stats válidas.")
if only_date_updates:
    print(f"📅 {len(only_date_updates)} jogador(es) com apenas updated_at para atualizar.")

try:
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    # ===============================
    # UPDATE RANKING_SQUAD
    # ===============================
    sql = """
    INSERT INTO ranking_squad
    (nick, partidas, kr, vitorias, kills, dano_medio,
     assists, headshots, revives, kill_dist_max, top10, atualizado_em, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (nick) DO UPDATE SET
    partidas=EXCLUDED.partidas,
    kr=EXCLUDED.kr,
    vitorias=EXCLUDED.vitorias,
    kills=EXCLUDED.kills,
    dano_medio=EXCLUDED.dano_medio,
    assists=EXCLUDED.assists,
    headshots=EXCLUDED.headshots,
    revives=EXCLUDED.revives,
    kill_dist_max=EXCLUDED.kill_dist_max,
    top10=EXCLUDED.top10,
    atualizado_em = CASE
        WHEN EXCLUDED.partidas > 0 THEN EXCLUDED.atualizado_em
        ELSE ranking_squad.atualizado_em
    END,
    updated_at = CASE
        updated_at = COALESCE(EXCLUDED.updated_at, ranking_squad.updated_at)
        ELSE ranking_squad.updated_at
    END
    """
    cursor.executemany(sql, resultados)

    if only_date_updates:
        for updated_at, nick in only_date_updates:
            cursor.execute(
                "UPDATE ranking_squad SET updated_at = %s, atualizado_em = atualizado_em WHERE nick = %s AND updated_at IS NULL",
                (updated_at, nick)
            )
            print(f"📅 updated_at atualizado para {nick}: {updated_at}")

    semana_atual = get_segunda_feira()
    semana_anterior = semana_atual - timedelta(weeks=1)
    print(f"📊 Salvando snapshot semanal para semana de {semana_atual}...")

    cursor.execute(
        "SELECT COUNT(*) FROM ranking_semanal WHERE semana = %s",
        (semana_atual,)
    )
    ja_existe_semana_atual = cursor.fetchone()[0] > 0

    if not ja_existe_semana_atual:
        print(f"🔄 Primeiro sync da semana. Atualizando snapshot de {semana_anterior}...")
        sql_atualiza_anterior = """
        INSERT INTO ranking_semanal
        (nick, semana, partidas, kr, vitorias, kills, dano_medio,
         assists, headshots, revives, kill_dist_max, top10, atualizado_em)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (nick, semana) DO UPDATE SET
        partidas=EXCLUDED.partidas,
        kr=EXCLUDED.kr,
        vitorias=EXCLUDED.vitorias,
        kills=EXCLUDED.kills,
        dano_medio=EXCLUDED.dano_medio,
        assists=EXCLUDED.assists,
        headshots=EXCLUDED.headshots,
        revives=EXCLUDED.revives,
        kill_dist_max=EXCLUDED.kill_dist_max,
        top10=EXCLUDED.top10,
        atualizado_em=EXCLUDED.atualizado_em
        """
        resultados_anterior = [
            (r[0], semana_anterior, r[1], r[2], r[3], r[4],
             r[5], r[6], r[7], r[8], r[9], r[10], r[11])
            for r in resultados
        ]
        cursor.executemany(sql_atualiza_anterior, resultados_anterior)
        print(f"✅ Snapshot de {semana_anterior} atualizado como âncora.")

    sql_semanal = """
    INSERT INTO ranking_semanal
    (nick, semana, partidas, kr, vitorias, kills, dano_medio,
     assists, headshots, revives, kill_dist_max, top10, atualizado_em)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (nick, semana) DO UPDATE SET
    partidas=EXCLUDED.partidas,
    kr=EXCLUDED.kr,
    vitorias=EXCLUDED.vitorias,
    kills=EXCLUDED.kills,
    dano_medio=EXCLUDED.dano_medio,
    assists=EXCLUDED.assists,
    headshots=EXCLUDED.headshots,
    revives=EXCLUDED.revives,
    kill_dist_max=EXCLUDED.kill_dist_max,
    top10=EXCLUDED.top10,
    atualizado_em=EXCLUDED.atualizado_em
    """

    resultados_semanal = [
        (r[0], semana_atual, r[1], r[2], r[3], r[4],
         r[5], r[6], r[7], r[8], r[9], r[10], r[11])
        for r in resultados
    ]

    cursor.executemany(sql_semanal, resultados_semanal)
    conn.commit()

    cursor.close()
    conn.close()

    print("💾 Banco atualizado com sucesso!")

except Exception as e:
    print(f"💥 Erro no banco: {e}")

fim_total = time.time()
print(f"⏱ Tempo total: {round(fim_total - inicio_total, 2)} segundos")
