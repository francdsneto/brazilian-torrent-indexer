from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import Response
import requests
import sqlite3
import hashlib
import re
import threading
import asyncio
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import os

# Fuso horário padrão do Brasil para a raspagem agendada
try:
    import zoneinfo
    TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")
except ImportError:
    from datetime import timezone
    TZ = timezone(timedelta(hours=-3))

app = FastAPI()

DFINDEXER_URL = os.getenv("DFINDEXER_URL", "http://dfindexer:7006")
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")  # Chave do TMDB para tradução
DB_PATH = "/app/data/cache.db"

# ========================
# INDEXADORES SUPORTADOS PELO DFINDEXER
# ========================
INDEXERS =["starck", "rede", "tfilme", "portal", "xfilmes", "comando", "bludv"]

# ========================
# LOCKS & CONCURRENCY
# ========================
db_lock = threading.Lock()
active_tasks = set()
active_tasks_lock = threading.Lock()

# ========================
# DATABASE INIT & MIGRATION
# ========================
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL")
cursor = conn.cursor()

# 1. Cria a tabela padrão (se o banco não existir)
cursor.execute("""
CREATE TABLE IF NOT EXISTS torrents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    magnet TEXT UNIQUE,
    hash TEXT,
    size INTEGER,
    seeders INTEGER,
    leechers INTEGER,
    pub_date TEXT,
    created_at TEXT,
    query TEXT,
    year TEXT,
    imdb TEXT,
    details TEXT
)
""")
conn.commit()

# 2. MIGRATION: Atualiza o banco antigo automaticamente
cursor.execute("PRAGMA table_info(torrents)")
existing_columns = [col[1] for col in cursor.fetchall()]

if "size" not in existing_columns:
    cursor.execute("ALTER TABLE torrents ADD COLUMN size INTEGER DEFAULT 1073741824")
if "seeders" not in existing_columns:
    cursor.execute("ALTER TABLE torrents ADD COLUMN seeders INTEGER DEFAULT 0")
if "leechers" not in existing_columns:
    cursor.execute("ALTER TABLE torrents ADD COLUMN leechers INTEGER DEFAULT 0")
if "year" not in existing_columns:
    cursor.execute("ALTER TABLE torrents ADD COLUMN year TEXT")
if "imdb" not in existing_columns:
    cursor.execute("ALTER TABLE torrents ADD COLUMN imdb TEXT")
if "details" not in existing_columns:
    cursor.execute("ALTER TABLE torrents ADD COLUMN details TEXT")

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

def parse_size(size_str):
    if not size_str:
        return 1073741824
        
    if isinstance(size_str, (int, float)):
        return int(size_str)
        
    size_str = str(size_str).upper().strip()
    try:
        match = re.match(r"([\d\.]+)\s*([A-Z]*)", size_str)
        if not match:
            return 1073741824
            
        val = float(match.group(1))
        unit = match.group(2)
        
        if "TB" in unit: val *= (1024 ** 4)
        elif "GB" in unit: val *= (1024 ** 3)
        elif "MB" in unit: val *= (1024 ** 2)
        elif "KB" in unit: val *= 1024
        
        return int(val)
    except:
        return 1073741824

def insert_if_new(item, query):
    magnet = item.get("magnet_link")
    if not magnet:
        return False

    # Força o hash a ser minúsculo para evitar duplicatas por letras maiúsculas
    h = item.get("info_hash")
    if h:
        h = str(h).lower().strip()
    else:
        h = hash_magnet(magnet)

    with db_lock:
        # Checa se o hash já existe
        cursor.execute("SELECT 1 FROM torrents WHERE hash = ?", (h,))
        if cursor.fetchone():
            return False
            
        raw_size = item.get("size")
        size_bytes = parse_size(raw_size)
        
        title = item.get("title_processed") or item.get("original_title") or item.get("title") or "Unknown Title"
        title = title.strip()
        
        # Filtro Anti-Duplicatas: Se já temos esse título com o mesmo tamanho no DB, não salva de novo!
        cursor.execute("SELECT 1 FROM torrents WHERE title = ? AND size = ?", (title, size_bytes))
        if cursor.fetchone():
            return False

        seeders = int(item.get("seed_count") or 0)
        leechers = int(item.get("leech_count") or 0)
        
        year = str(item.get("year") or "")
        imdb = str(item.get("imdb") or "")
        details = str(item.get("details") or "")

        cursor.execute("""
        INSERT INTO torrents (title, magnet, hash, size, seeders, leechers, pub_date, created_at, query, year, imdb, details)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            title, magnet, h, size_bytes, seeders, leechers,
            item.get("date"), datetime.utcnow().isoformat(),
            query, year, imdb, details
        ))
        conn.commit()
        return True


def fetch_dfindexer(query=None, pages=1, indexer_type=None):
    results =[]

    for page in range(1, pages + 1):
        try:
            if indexer_type:
                url = f"{DFINDEXER_URL}/indexers/{indexer_type}?page={page}&use_flaresolverr=true"
            else:
                url = f"{DFINDEXER_URL}/indexer?page={page}&use_flaresolverr=true"
                
            if query:
                url += f"&q={query}"

            r = requests.get(url, timeout=120)
            if r.status_code == 200:
                data = r.json()
                results.extend(data.get("results",[]))
            else:
                print(f"Erro DFIndexer ({indexer_type or 'padrão'} - pág {page}): HTTP {r.status_code}")

        except Exception as e:
            print(f"Erro DFIndexer ({indexer_type or 'padrão'} - pág {page}):", e)

    return results


def get_from_db(query=None, imdbid=None, limit=50):
    with db_lock:
        if imdbid:
            # O Radarr envia o ID com "tt" na frente (ex: tt10857160)
            clean_imdb = f"%{imdbid.replace('tt', '')}%"
            cursor.execute("""
            SELECT title, magnet, hash, size, seeders, leechers, pub_date, year, imdb, details 
            FROM torrents
            WHERE imdb LIKE ? OR query = ?
            ORDER BY datetime(created_at) DESC
            LIMIT ?
            """, (clean_imdb, imdbid, limit))
            return cursor.fetchall()

        elif query:
            # Substitui espaços por '%' para achar via comando LIKE
            like_query = f"%{query.replace(' ', '%')}%"
            cursor.execute("""
            SELECT title, magnet, hash, size, seeders, leechers, pub_date, year, imdb, details 
            FROM torrents
            WHERE query = ? OR title LIKE ?
            ORDER BY datetime(created_at) DESC
            LIMIT ?
            """, (query, like_query, limit))
            return cursor.fetchall()
            
        else:
            # RSS Padrão (Sem query e sem imdbid)
            cursor.execute("""
            SELECT title, magnet, hash, size, seeders, leechers, pub_date, year, imdb, details 
            FROM torrents
            ORDER BY datetime(created_at) DESC
            LIMIT ?
            """, (limit,))
            return cursor.fetchall()


def should_refresh(query):
    if not query:
        return False
        
    with db_lock:
        cursor.execute("""
        SELECT MAX(created_at) FROM torrents WHERE query = ?
        """, (query,))
        row = cursor.fetchone()

    if not row or not row[0]:
        return True

    last = datetime.fromisoformat(row[0])
    return datetime.utcnow() - last > timedelta(days=7)


# ========================
# BACKGROUND TASKS & SCHEDULES
# ========================
def background_scrape_and_save(query):
    with active_tasks_lock:
        if query in active_tasks:
            return
        active_tasks.add(query)

    try:
        for idx in INDEXERS:
            results = fetch_dfindexer(query, pages=2, indexer_type=idx)
            for item in results:
                insert_if_new(item, query)
    except Exception as e:
        print(f"Erro no background scrape da query '{query}':", e)
    finally:
        with active_tasks_lock:
            active_tasks.discard(query)


def run_scheduled_scrape():
    """Função invocada pelo scheduler para raspar todos os indexadores"""
    total_inseridos = 0
    for idx in INDEXERS:
        print(f"[{datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')}] Raspando novidades do site: {idx} ...")
        try:
            results = fetch_dfindexer(query=None, pages=2, indexer_type=idx)
            for item in results:
                if insert_if_new(item, None):
                    total_inseridos += 1
        except Exception as e:
             print(f"Erro ao raspar indexador {idx}: {e}")
             
    print(f"[{datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')}] Raspagem agendada finalizada. {total_inseridos} novos torrents adicionados.")


async def scheduled_scraper_task():
    """Loop assíncrono que acorda às 00h, 12h e 18h para iniciar o scrape"""
    while True:
        now = datetime.now(TZ)
        target_hours = [0, 12, 18]
        
        next_run = None
        for h in target_hours:
            candidate = now.replace(hour=h, minute=0, second=0, microsecond=0)
            if candidate > now:
                next_run = candidate
                break
                
        if next_run is None:
            next_run = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            
        wait_seconds = (next_run - now).total_seconds()
        print(f"[{datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')}] Próxima raspagem agendada para: {next_run.strftime('%H:%M:%S')} ({wait_seconds:.0f}s restantes)")
        
        await asyncio.sleep(wait_seconds)
        
        print(f"[{datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')}] Iniciando raspagem agendada...")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, run_scheduled_scrape)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(scheduled_scraper_task())


# ========================
# TRADUÇÃO DE IDIOMA (TMDB)
# ========================
def get_ptbr_title(imdbid=None, tvdbid=None, tmdbid=None, is_tv=False):
    """Consulta o TMDB para traduzir o ID recebido para o Título em Português do Brasil."""
    if not TMDB_API_KEY:
        return None
        
    try:
        headers = {"accept": "application/json"}
        url = None
        
        if imdbid:
            url = f"https://api.themoviedb.org/3/find/{imdbid}?api_key={TMDB_API_KEY}&external_source=imdb_id&language=pt-BR"
        elif tvdbid:
            url = f"https://api.themoviedb.org/3/find/{tvdbid}?api_key={TMDB_API_KEY}&external_source=tvdb_id&language=pt-BR"
            
        if url:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if data.get("movie_results"):
                    return data["movie_results"][0].get("title")
                elif data.get("tv_results"):
                    return data["tv_results"][0].get("name")
                    
        if tmdbid:
            type_str = "tv" if is_tv else "movie"
            url = f"https://api.themoviedb.org/3/{type_str}/{tmdbid}?api_key={TMDB_API_KEY}&language=pt-BR"
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                data = r.json()
                return data.get("name") if is_tv else data.get("title")
                
    except Exception as e:
        print(f"Erro ao buscar tradução no TMDB: {e}")
        
    return None


# ========================
# XML GENERATOR (TORZNAB)
# ========================
def generate_rss(items):
    rss = ET.Element("rss", {
        "version": "2.0",
        "xmlns:torznab": "http://torznab.com/schemas/2015/feed"
    })
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = "DFIndexer Proxy"
    ET.SubElement(channel, "description").text = "Proxy wrapper for DFIndexer"
    ET.SubElement(channel, "link").text = DFINDEXER_URL

    # Variáveis de controle para filtrar duplicatas antigas já armazenadas no DB
    seen_hashes = set()
    seen_titles_sizes = set()

    for row in items:
        title, magnet, hash_val, size, seeders, leechers, pub_date, year, imdb, details = row
        
        # Ignora se o Hash for repetido no mesmo feed
        clean_hash = str(hash_val).lower().strip() if hash_val else ""
        if clean_hash and clean_hash in seen_hashes:
            continue
        seen_hashes.add(clean_hash)
        
        # Ignora se o Título e o Tamanho forem idênticos (Cópia de outro site)
        title_key = f"{str(title).lower().strip()}_{size}"
        if title_key in seen_titles_sizes:
            continue
        seen_titles_sizes.add(title_key)
        
        item = ET.SubElement(channel, "item")

        ET.SubElement(item, "title").text = title
        ET.SubElement(item, "guid").text = magnet
        ET.SubElement(item, "link").text = magnet
        
        if details:
            ET.SubElement(item, "comments").text = details
        
        ET.SubElement(item, "size").text = str(size)
        ET.SubElement(item, "enclosure", {
            "url": magnet,
            "length": str(size),
            "type": "application/x-bittorrent"
        })

        try:
            dt = datetime.fromisoformat(pub_date)
        except:
            dt = datetime.utcnow()

        ET.SubElement(item, "pubDate").text = dt.strftime("%a, %d %b %Y %H:%M:%S GMT")

        ET.SubElement(item, "torznab:attr", {"name": "seeders", "value": str(seeders)})
        ET.SubElement(item, "torznab:attr", {"name": "peers", "value": str(seeders + leechers)})
        ET.SubElement(item, "torznab:attr", {"name": "infohash", "value": hash_val})
        ET.SubElement(item, "torznab:attr", {"name": "magneturl", "value": magnet})
        
        if year and year != "None":
            ET.SubElement(item, "torznab:attr", {"name": "year", "value": str(year)})
            
        if imdb and imdb != "None":
            imdb_id = imdb.replace("tt", "") if imdb.startswith("tt") else imdb
            if imdb_id.strip():
                ET.SubElement(item, "torznab:attr", {"name": "imdb", "value": imdb_id.strip()})
        
        # O DFIndexer geralmente retorna arquivos mistos, expor todas as categorias 
        # garante que Sonarr e Radarr analisem o título de tudo que for encontrado.
        ET.SubElement(item, "torznab:attr", {"name": "category", "value": "2000"})
        ET.SubElement(item, "torznab:attr", {"name": "category", "value": "2030"})
        ET.SubElement(item, "torznab:attr", {"name": "category", "value": "2040"})
        ET.SubElement(item, "torznab:attr", {"name": "category", "value": "5000"})
        ET.SubElement(item, "torznab:attr", {"name": "category", "value": "5030"})
        ET.SubElement(item, "torznab:attr", {"name": "category", "value": "5040"})
        ET.SubElement(item, "torznab:attr", {"name": "category", "value": "5070"})
        ET.SubElement(item, "torznab:attr", {"name": "category", "value": "8000"})

    return ET.tostring(rss, encoding="utf-8")


# ========================
# ROUTES
# ========================
@app.get("/api")
async def torznab(request: Request, background_tasks: BackgroundTasks):
    t = request.query_params.get("t")
    
    query_param = request.query_params.get("q")
    query = query_param.lower().strip() if query_param else None

    # Captura os IDs que o Sonarr/Radarr enviam
    imdbid = request.query_params.get("imdbid")
    tvdbid = request.query_params.get("tvdbid")
    tmdbid = request.query_params.get("tmdbid")

    # ========================
    # CAPS (Capabilities) - COM SEASON E EPISÓDIO
    # ========================
    if t == "caps":
        return Response(content="""<?xml version="1.0" encoding="UTF-8"?>
<caps>
  <server version="1.0" title="DFIndexer Proxy"/>
  <limits max="100" default="50"/>
  <searching>
    <search available="yes" supportedParams="q,imdbid"/>
    <tv-search available="yes" supportedParams="q,tvdbid,tmdbid,imdbid,season,ep"/>
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
      <subcat id="5070" name="TV/Anime"/>
    </category>
    <category id="8000" name="Other"/>
  </categories>
</caps>
""", media_type="application/xml")

    # ========================
    # SEARCH
    # ========================
    if t in ["search", "tvsearch", "movie"]:

        # CASO 1 — Busca RSS Vazia (quando não há texto nem ID)
        if not query and not imdbid and not tvdbid and not tmdbid:
            data = get_from_db(query=None, imdbid=None, limit=50)

            if not data:
                dummy_hash = "1234567890abcdef1234567890abcdef12345678"
                dummy_magnet = f"magnet:?xt=urn:btih:{dummy_hash}&dn=Bootstrapping"
                return Response(content=generate_rss([
                    ("Bootstrapping System Proxy...", dummy_magnet, dummy_hash, 1073741824, 10, 2, datetime.utcnow().isoformat(), "", "", "")
                ]), media_type="application/xml")

            return Response(content=generate_rss(data), media_type="application/xml")

        # CASO 2 e 3 — Tradução Dinâmica e Busca
        search_title = query
        
        # Se não temos um texto de busca (apenas ID), o proxy vai traduzir!
        if not search_title and (imdbid or tvdbid or tmdbid):
            is_tv = (t == "tvsearch")
            translated = get_ptbr_title(imdbid=imdbid, tvdbid=tvdbid, tmdbid=tmdbid, is_tv=is_tv)
            if translated:
                search_title = translated.lower()
                print(f"TMDB Traduziu o ID para: {search_title}")

        # Termo final que vai pro banco de dados e pro DFIndexer raspar
        final_search_term = search_title if search_title else imdbid
        
        # Tenta pegar no DB
        data = get_from_db(query=search_title, imdbid=imdbid)

        # Se não tem no banco ou precisa renovar, raspa (com o nome já traduzido!) em background
        if not data or should_refresh(final_search_term):
            if final_search_term:
                background_tasks.add_task(background_scrape_and_save, final_search_term)

        # Retorna instantaneamente os itens do banco (evita timeout)
        return Response(content=generate_rss(data), media_type="application/xml")

    return {"error": "invalid request, 't' parameter unsupported or missing"}
