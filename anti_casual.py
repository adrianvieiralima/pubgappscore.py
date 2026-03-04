import requests
import time
import os
import psycopg2

API_KEY = os.getenv("PUBG_API_KEY") or "SUA_CHAVE_AQUI"
DATABASE_URL = os.getenv("DATABASE_URL")
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

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/vnd.api+json"
}

def get(url):
    r = requests.get(url, headers=HEADERS)
    if r.status_code == 200:
        return r.json()

    print("Erro API:", r.status_code)
    return None


def processar_player(conn, player_name, player_id):
    print(f"\n🔎 Player: {player_name}")
    cur = conn.cursor()

    player_data = get(f"https://api.pubg.com/shards/{SHARD}/players/{player_id}")
    if not player_data:
        print("Erro ao buscar player.")
        return 0

    matches = player_data["data"]["relationships"]["matches"]["data"]
    print("Matches retornadas:", len(matches))

    penalidades = 0

    for m in matches:
        match_id = m["id"]

        # Evita processar a mesma partida duas vezes para o mesmo player
        cur.execute("""
            SELECT 1 FROM matches_processadas 
            WHERE match_id = %s AND player_name = %s
        """, (match_id, player_name))

        if cur.fetchone():
            continue

        match_data = get(f"https://api.pubg.com/shards/{SHARD}/matches/{match_id}")
        if not match_data:
            continue

        attr = match_data["data"]["attributes"]

        # 🔥 SOMENTE SQUAD (igual seu script original)
        if attr.get("gameMode") != "squad":
            continue

        participants = [
            x for x in match_data["included"]
            if x["type"] == "participant"
        ]

        humanos = sum(
            1 for p in participants
            if p["attributes"]["stats"].get("playerId", "").startswith("account.")
        )

        bots = len(participants) - humanos

        print(f"\nMatch: {match_id}")
        print("GameMode:", attr.get("gameMode"))
        print("Map:", attr.get("mapName"))
        print("Humanos:", humanos)
        print("Bots:", bots)

        # 🔥 SUA REGRA ORIGINAL (inalterada)
        if (
            attr.get("matchType") == "casual"
            or humanos <= 12
        ):
            print(">>> CASUAL SQUAD TPP DETECTADO <<<")

            # pegar stats do player nessa partida
            p_stats = next(
                (x["attributes"]["stats"] for x in participants
                 if x["attributes"]["stats"].get("playerId") == player_id),
                None
            )

            if p_stats:
    kills = p_stats.get("kills", 0)
    dano = p_stats.get("damageDealt", 0)
    # --- NOVAS VARIÁVEIS CAPTURADAS ---
    vitorias = 1 if p_stats.get("winPlace") == 1 else 0
    assists = p_stats.get("assists", 0)
    headshots = p_stats.get("headshotKills", 0)
    revives = p_stats.get("revives", 0)
    dist_max = p_stats.get("longestKill", 0)

    # Penalidade negativa (sua regra original)
    score_penalidade = (kills * 10) + (dano * 0.1)

    # --- UPDATE COMPLETO ---
    cur.execute("""
        UPDATE ranking_bot SET
            kills = kills - %s,
            score = score - %s,
            partidas = partidas + 1,
            vitorias = vitorias + %s,
            dano_medio = dano_medio + %s,
            assists = assists + %s,
            headshots = headshots + %s,
            revives = revives + %s,
            kill_dist_max = GREATEST(kill_dist_max, %s),
            atualizado_em = NOW()
        WHERE nick = %s
    """, (
        kills, score_penalidade, vitorias, dano, 
        assists, headshots, revives, dist_max, player_name
    ))

        # registra como processada
        cur.execute("""
            INSERT INTO matches_processadas (match_id, player_name)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (match_id, player_name))

        conn.commit()
        time.sleep(0.7)

    print(f"\n🎯 Penalidades aplicadas para {player_name}: {penalidades}")
    return penalidades


# EXECUÇÃO
if not DATABASE_URL:
    print("❌ DATABASE_URL não configurado.")
    exit()

conn = psycopg2.connect(DATABASE_URL)

total_geral = 0

for name, pid in PLAYERS.items():
    total_geral += processar_player(conn, name, pid)

conn.close()

print("\n===================================")
print("TOTAL PENALIDADES APLICADAS:", total_geral)
print("===================================")
