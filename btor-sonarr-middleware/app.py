from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import Response
import requests
import sqlite3
import hashlib
import re
import asyncio
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

try:
    import zoneinfo
    TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")
except ImportError:
    from datetime import timezone
    TZ = timezone(timedelta(hours=-3))

app = FastAPI()

BETOR_JSON_URL = "https://catalogo.betor.top/static/data/items.json"
DB_PATH = "/app/data/betor_cache.db"

# ========================
# DATABASE INIT
# ========================
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL")
cursor = conn.cursor()

# Tabela com suporte a imdb e tmdb, e a chave única composta
cursor.execute("""
CREATE TABLE IF NOT EXISTS torrents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    magnet TEXT,
    hash TEXT,
    size INTEGER,
    seeders INTEGER,
    leechers INTEGER,
    pub_date TEXT,
    imdb TEXT,
    tmdb TEXT,
    created_at TEXT,
    UNIQUE(hash, title)
)
""")
conn.commit()

# ========================
# HELPERS
# ========================
def hash_magnet(magnet: str):
    try:
        if "urn:btih:" in magnet:
            return magnet.split("urn:btih:")[1].split("&")[0].lower()
    except:
        pass
    return hashlib.sha1(magnet.encode()).hexdigest()

def extract_episodes(files):
    """Procura por SxxExx ou equivalentes nos arquivos do torrent"""
    episodes =[]
    if not files:
        return episodes
        
    for f in files:
        # Pega formatos: S01E01, s01e01, 1x01
        match = re.search(r"([Ss]\d{1,2}[EeXx]\d{1,2})", f)
        if match:
            episodes.append(f)
    return episodes

# ========================
# INGESTÃO DE DADOS (PARSER)
# ========================
def fetch_and_parse_betor():
    print(f"[{datetime.now(TZ).strftime('%H:%M:%S')}] Baixando JSON do BeTor...")
    try:
        r = requests.get(BETOR_JSON_URL, timeout=120)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print("Erro ao baixar BeTor JSON:", e)
        return

    novos_inseridos = 0
    
    for item in data:
        # Extrai os IDs root
        imdb = str(item.get("imdb_id") or "")
        tmdb = str(item.get("tmdb_id") or "")
        
        # Pega o nome oficial como Fallback
        info = item.get("info") or {}
        fallback_title = info.get("title") or info.get("name") or info.get("original_title") or "Unknown"
        
        # O Pulo do Gato: iterar sobre os providers primeiro!
        for provider in item.get("providers", []):
            
            for torrent in provider.get("torrents",[]):
                magnet = torrent.get("magnet_uri")
                if not magnet:
                    continue
                    
                hash_val = hash_magnet(magnet)
                size = torrent.get("torrent_size") or 0
                seeds = torrent.get("torrent_num_seeds") or 0
                peers = torrent.get("torrent_num_peers") or 0
                date = torrent.get("inserted_at") or datetime.utcnow().isoformat()
                
                t_name = torrent.get("torrent_name") or fallback_title
                files = torrent.get("torrent_files") or[]
                
                # Tenta extrair episódios dos arquivos
                episodes = extract_episodes(files)
                
                try:
                    if episodes:
                        # Se for temporada, divide o tamanho total pelos episódios
                        ep_size = int(size / len(episodes)) if size else 0
                        
                        for ep_file in episodes:
                            # Pega só o nome do arquivo, removendo diretórios
                            clean_title = ep_file.split("/")[-1].split("\\")[-1]
                            
                            cursor.execute("""
                            INSERT OR IGNORE INTO torrents 
                            (title, magnet, hash, size, seeders, leechers, pub_date, imdb, tmdb, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (clean_title, magnet, hash_val, ep_size, seeds, peers, date, imdb, tmdb, datetime.utcnow().isoformat()))
                            
                            if cursor.rowcount > 0: novos_inseridos += 1
                    else:
                        # É um filme ou episódio único
                        cursor.execute("""
                        INSERT OR IGNORE INTO torrents 
                        (title, magnet, hash, size, seeders, leechers, pub_date, imdb, tmdb, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (t_name, magnet, hash_val, size, seeds, peers, date, imdb, tmdb, datetime.utcnow().isoformat()))
                        
                        if cursor.rowcount > 0: novos_inseridos += 1
                        
                except sqlite3.Error:
                    pass # Ignora erros de constraint (duplicatas)

    conn.commit()
    print(f"[{datetime.now(TZ).strftime('%H:%M:%S')}] Ingestão finalizada! {novos_inseridos} novos torrents/episódios indexados.")

# ========================
# BACKGROUND SCHEDULER
# ========================
async def scheduled_scraper_task():
    """Roda a ingestão assim que o server liga, e depois a cada 12 horas"""
    loop = asyncio.get_running_loop()
    while True:
        await loop.run_in_executor(None, fetch_and_parse_betor)
        print(f"[{datetime.now(TZ).strftime('%H:%M:%S')}] Aguardando 12 horas para a próxima atualização do BeTor...")
        await asyncio.sleep(43200) # 12 horas

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(scheduled_scraper_task())

# ========================
# DATABASE QUERY
# ========================
def get_from_db(query=None, imdbid=None, tmdbid=None, limit=100):
    if imdbid:
        clean_imdb = f"%{imdbid.replace('tt', '')}%"
        cursor.execute("""
        SELECT title, magnet, hash, size, seeders, leechers, pub_date, imdb, tmdb 
        FROM torrents WHERE imdb LIKE ? ORDER BY seeders DESC LIMIT ?
        """, (clean_imdb, limit))
        return cursor.fetchall()
        
    elif tmdbid:
        cursor.execute("""
        SELECT title, magnet, hash, size, seeders, leechers, pub_date, imdb, tmdb 
        FROM torrents WHERE tmdb = ? ORDER BY seeders DESC LIMIT ?
        """, (tmdbid, limit))
        return cursor.fetchall()

    elif query:
        like_query = f"%{query.replace(' ', '%')}%"
        cursor.execute("""
        SELECT title, magnet, hash, size, seeders, leechers, pub_date, imdb, tmdb 
        FROM torrents WHERE title LIKE ? ORDER BY seeders DESC LIMIT ?
        """, (like_query, limit))
        return cursor.fetchall()
        
    else:
        cursor.execute("""
        SELECT title, magnet, hash, size, seeders, leechers, pub_date, imdb, tmdb 
        FROM torrents ORDER BY datetime(created_at) DESC LIMIT ?
        """, (limit,))
        return cursor.fetchall()

# ========================
# XML GENERATOR (TORZNAB)
# ========================
def generate_rss(items):
    rss = ET.Element("rss", {"version": "2.0", "xmlns:torznab": "http://torznab.com/schemas/2015/feed"})
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = "BeTor Proxy"
    ET.SubElement(channel, "description").text = "Conversor de Season Packs BeTor para Sonarr/Radarr"
    ET.SubElement(channel, "link").text = BETOR_JSON_URL

    for row in items:
        title, magnet, hash_val, size, seeders, leechers, pub_date, imdb, tmdb = row
        
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = title
        
        # GUID composto por hash e titulo garante que os episódios explodidos não conflitem no Sonarr
        ET.SubElement(item, "guid", {"isPermaLink": "false"}).text = f"{hash_val}-{title}"
        
        ET.SubElement(item, "link").text = magnet
        ET.SubElement(item, "size").text = str(size)
        ET.SubElement(item, "enclosure", {"url": magnet, "length": str(size), "type": "application/x-bittorrent"})

        try:
            dt = datetime.fromisoformat(pub_date)
        except:
            dt = datetime.utcnow()
        ET.SubElement(item, "pubDate").text = dt.strftime("%a, %d %b %Y %H:%M:%S GMT")

        ET.SubElement(item, "torznab:attr", {"name": "seeders", "value": str(seeders)})
        ET.SubElement(item, "torznab:attr", {"name": "peers", "value": str(seeders + leechers)})
        ET.SubElement(item, "torznab:attr", {"name": "infohash", "value": hash_val})
        ET.SubElement(item, "torznab:attr", {"name": "magneturl", "value": magnet})
        
        if imdb and imdb != "None":
            imdb_id = imdb.replace("tt", "") if imdb.startswith("tt") else imdb
            if imdb_id.strip():
                ET.SubElement(item, "torznab:attr", {"name": "imdb", "value": imdb_id.strip()})
                
        if tmdb and tmdb != "None":
            if tmdb.strip():
                ET.SubElement(item, "torznab:attr", {"name": "tmdbid", "value": tmdb.strip()})
        
        ET.SubElement(item, "torznab:attr", {"name": "category", "value": "2000"}) # Filmes
        ET.SubElement(item, "torznab:attr", {"name": "category", "value": "5000"}) # Series
        ET.SubElement(item, "torznab:attr", {"name": "category", "value": "5030"}) # Series SD
        ET.SubElement(item, "torznab:attr", {"name": "category", "value": "5040"}) # Series HD

    return ET.tostring(rss, encoding="utf-8")


# ========================
# ROUTES
# ========================
@app.get("/api")
async def torznab(request: Request):
    t = request.query_params.get("t")
    query_param = request.query_params.get("q")
    query = query_param.lower().strip() if query_param else None

    # O BeTor agora possui IMDB ID e TMDB ID
    imdbid = request.query_params.get("imdbid")
    tmdbid = request.query_params.get("tmdbid")

    if t == "caps":
        return Response(content="""<?xml version="1.0" encoding="UTF-8"?>
<caps>
  <server version="1.0" title="BeTor Proxy"/>
  <limits max="100" default="100"/>
  <searching>
    <search available="yes" supportedParams="q,imdbid,tmdbid"/>
    <tv-search available="yes" supportedParams="q,imdbid,tmdbid,season,ep"/>
    <movie-search available="yes" supportedParams="q,imdbid,tmdbid"/>
  </searching>
  <categories>
    <category id="2000" name="Movies">
      <subcat id="2030" name="Movies/SD"/>
      <subcat id="2040" name="Movies/HD"/>
    </category>
    <category id="5000" name="TV">
      <subcat id="5030" name="TV/SD"/>
      <subcat id="5040" name="TV/HD"/>
    </category>
  </categories>
</caps>
""", media_type="application/xml")

    if t in ["search", "tvsearch", "movie"]:
        data = get_from_db(query=query, imdbid=imdbid, tmdbid=tmdbid)
        
        # Retorna um arquivo fake para bootstrapping se o banco estiver vazio
        if not data and not query and not imdbid and not tmdbid:
            dummy_hash = "1234567890abcdef1234567890abcdef12345678"
            dummy_magnet = f"magnet:?xt=urn:btih:{dummy_hash}&dn=Bootstrapping"
            return Response(content=generate_rss([
                ("Aguarde a Ingestao Inicial do BeTor...", dummy_magnet, dummy_hash, 1073741824, 10, 2, datetime.utcnow().isoformat(), "", "")
            ]), media_type="application/xml")

        return Response(content=generate_rss(data), media_type="application/xml")

    return {"error": "Parâmetro 't' inválido ou ausente"}
