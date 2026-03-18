import asyncio
import aiohttp
import os
import psycopg2
from psycopg2.pool import ThreadedConnectionPool

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

HEADERS = {"Authorization": f"Bearer {API_KEY}", "Accept": "application/vnd.api+json"}

sem = None

async def get(session, url):
    for attempt in range(3):
        async with sem:
            async with session.get(url, headers=HEADERS) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
                elif r.status == 429:
                    wait = int(r.headers.get("Retry-After", 10))
                    print(f"⏳ Rate limit atingido, aguardando {wait}s... (tentativa {attempt + 1}/3)")
                    await asyncio.sleep(wait)
                elif r.status == 401:
                    print(f"❌ API Key inválida ou expirada.")
                    return None
                elif r.status == 404:
                    print(f"⚠️ Recurso não encontrado: {url}")
                    return None
                else:
                    print(f"❌ Erro {r.status} em {url} (tentativa {attempt + 1}/3)")
                    await asyncio.sleep(2)
    print(f"❌ Falhou após 3 tentativas: {url}")
    return None

async def processar_player(session, pool, player_name, player_id):
    print(f"\n🔎 Processando: {player_name}")
    conn = pool.getconn()
    cur = conn.cursor()

    try:
        player_data = await get(session, f"https://api.pubg.com/shards/{SHARD}/players/{player_id}")
        if not player_data: return 0

        matches = player_data["data"]["relationships"]["matches"]["data"]
        print(f"📋 {player_name}: {len(matches)} partidas encontradas na API")

        penalidades = 0
        ignoradas_ja_processada = 0
        ignoradas_modo_errado = 0
        ignoradas_nao_airoyale = 0

        for m in matches:
            match_id = m["id"]

            cur.execute("SELECT 1 FROM matches_processadas WHERE match_id = %s AND player_name = %s", (match_id, player_name))
            if cur.fetchone():
                print(f"   ⏭️  {match_id} → já processada, ignorando")
                ignoradas_ja_processada += 1
                continue

            match_data = await get(session, f"https://api.pubg.com/shards/{SHARD}/matches/{match_id}")
            if not match_data: continue

            attr = match_data["data"]["attributes"]

            if attr.get("gameMode") != "squad":
                print(f"   ❌ {match_id} → modo errado ({attr.get('gameMode')}), ignorando")
                ignoradas_modo_errado += 1
                cur.execute("INSERT INTO matches_processadas (match_id, player_name) VALUES (%s, %s) ON CONFLICT DO NOTHING", (match_id, player_name))
                conn.commit()
                continue

            if attr.get("matchType") != "airoyale":
                print(f"   ❌ {match_id} → não é airoyale (matchType={attr.get('matchType')}), ignorando")
                ignoradas_nao_airoyale += 1
                continue

            print(f"   ✅ {match_id} → airoyale confirmado, processando")
            participants = [x for x in match_data["included"] if x["type"] == "participant"]
            p_stats = next((x["attributes"]["stats"] for x in participants if x["attributes"]["stats"].get("playerId") == player_id), None)

            if p_stats:
                kills = p_stats.get("kills", 0)
                dano = p_stats.get("damageDealt", 0)
                score_penalidade = (kills * 10) + (dano * 0.1)

                cur.execute("""
                    UPDATE ranking_bot SET
