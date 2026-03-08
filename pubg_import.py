import os
import time
import requests
import psycopg2
from datetime import datetime

DATABASE_URL = os.environ.get("DATABASE_URL")
API_KEY = os.environ.get("PUBG_API_KEY")
BASE_URL = "https://api.pubg.com/shards/steam"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/vnd.api+json"
}

players_names = [
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


# =====================================
# FUNÇÃO PRINCIPAL (CHAMADA PELO SITE)
# =====================================

def atualizar_ranking():

    inicio_total = time.time()
    print("🚀 Atualizando ranking PUBG...")

    # Detectar temporada
    res_season = fazer_requisicao(f"{BASE_URL}/seasons")

    current_season_id = next(
        (s["id"] for s in res_season.json()["data"]
         if s["attributes"]["isCurrentSeason"]),
        ""
    )

    print(f"📅 Temporada atual: {current_season_id}")

    # ===============================
    # BUSCAR IDS
    # ===============================

    mapping_id_name = {}

    for grupo in dividir_lista(players_names, 10):
        nomes = ",".join(grupo)

        res = fazer_requisicao(f"{BASE_URL}/players?filter[playerNames]={nomes}")

        if res and res.status_code == 200:
            for p in res.json()["data"]:
                mapping_id_name[p["id"]] = p["attributes"]["name"]

    print(f"✅ {len(mapping_id_name)} jogadores encontrados")

    # ===============================
    # BUSCAR STATS
    # ===============================

    resultados = []

    for grupo_ids in dividir_lista(list(mapping_id_name.keys()), 10):

        ids_string = ",".join(grupo_ids)

        url_stats = f"{BASE_URL}/seasons/{current_season_id}/gameMode/squad/players?filter[playerIds]={ids_string}"

        res = fazer_requisicao(url_stats)

        if res and res.status_code == 200:

            for p_data in res.json().get("data", []):

                p_id = p_data["relationships"]["player"]["data"]["id"]

                player_name = mapping_id_name.get(p_id)

                stats = p_data["attributes"]["gameModeStats"]

                partidas = stats.get("roundsPlayed", 0)

                if partidas == 0:
                    continue

                kills = stats.get("kills", 0)
                vitorias = stats.get("wins", 0)
                assists = stats.get("assists", 0)
                headshots = stats.get("headshotKills", 0)
                revives = stats.get("revives", 0)
                dano_total = stats.get("damageDealt", 0)
                dist_max = stats.get("longestKill", 0.0)

                kr = round(kills / partidas, 2)
                dano_medio = int(dano_total / partidas)

                resultados.append((
                    player_name,
                    partidas,
                    kr,
                    vitorias,
                    kills,
                    dano_medio,
                    assists,
                    headshots,
                    revives,
                    dist_max,
                    datetime.utcnow()
                ))

                print(f"⚡ {player_name} atualizado")

    # ===============================
    # SALVAR NO BANCO
    # ===============================

    if resultados:

        try:

            conn = psycopg2.connect(DATABASE_URL)

            cursor = conn.cursor()

            sql = """
            INSERT INTO ranking_squad
            (nick, partidas, kr, vitorias, kills, dano_medio,
            assists, headshots, revives, kill_dist_max, atualizado_em)

            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)

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
            atualizado_em=EXCLUDED.atualizado_em
            """

            cursor.executemany(sql, resultados)

            conn.commit()

            cursor.close()
            conn.close()

            print("💾 Banco atualizado!")

        except Exception as e:

            print(f"💥 Erro no banco: {e}")

    fim_total = time.time()

    print(f"✅ Atualização finalizada em {round(fim_total - inicio_total,2)}s")
