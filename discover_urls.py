# discover_urls.py
import os
import sys
import requests
import configparser
import threading
import time
import random
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from url_normalize import url_normalize
import warnings
from bs4 import XMLParsedAsHTMLWarning

warnings.filterwarnings('ignore', category=XMLParsedAsHTMLWarning)

# SQLAlchemy関連のインポート
from sqlalchemy.dialects.postgresql import insert
from db_utils import get_local_db_session, CrawlQueue

# --- Globals for worker threads ---
urls_to_visit = set()
visited_urls = set()
lock = threading.Lock()

def worker_fetch_links(url: str, config, session) -> set:
    """
    Fetches a single URL, parses it for new links, normalizes them, and returns them as a set.
    """
    target_domain = config.get('General', 'TARGET_DOMAIN')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    request_delay = config.getfloat('General', 'REQUEST_DELAY_SECONDS', fallback=0.5)
    
    found_links = set()
    try:
        time.sleep(random.uniform(request_delay * 0.5, request_delay * 1.5))
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = session.get(url, timeout=request_timeout, headers=headers, allow_redirects=True)
        response.raise_for_status()
        
        content_type = response.headers.get("content-type", "").lower()

        if "html" in content_type:
            soup = BeautifulSoup(response.content, 'html5lib')
            for a_tag in soup.find_all('a', href=True):
                try:
                    link = urljoin(url, a_tag['href'])
                    normalized_link = url_normalize(link)
                    
                    if urlparse(normalized_link).netloc == target_domain:
                        found_links.add(normalized_link)
                except Exception:
                    pass # Ignore malformed URLs
                    
    except requests.exceptions.RequestException as req_e:
        print(f"  [!] Request Error: {url} - {req_e}", file=sys.stderr)
    except Exception as e:
        print(f"  [!] Unknown Error in worker: {url} - {e}", file=sys.stderr)
    
    return found_links

def main():
    """
    Main orchestrator for multi-level, parallel URL discovery.
    """
    global urls_to_visit, visited_urls
    
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    index_pages = [url_normalize(url) for url in config.get('Seeds', 'INDEX_PAGES').strip().split('\n') if url]
    max_workers = config.getint('Discoverer', 'MAX_DISCOVER_WORKERS')
    crawl_depth = config.getint('Discoverer', 'CRAWL_DEPTH')
    db_write_batch_size = config.getint('Discoverer', 'DB_WRITE_BATCH_SIZE', fallback=500)

    db_session = get_local_db_session()
    
    print(f"--- URL Discovery Started (Depth: {crawl_depth}) ---")

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.mount('http://', HTTPAdapter(max_retries=retries))
    
    urls_to_visit.update(index_pages)
    all_discovered_links = set()
    
    for depth in range(crawl_depth):
        if not urls_to_visit:
            print(f"[*] Depth {depth + 1}: No new URLs to discover. Stopping.")
            break
            
        with lock:
            current_batch = list(urls_to_visit - visited_urls)
            visited_urls.update(current_batch)
            urls_to_visit.clear()

        if not current_batch:
            print(f"[*] Depth {depth + 1}: No new unvisited URLs in this level. Stopping.")
            break

        print(f"\n[*] Depth {depth + 1}/{crawl_depth}: Discovering from {len(current_batch)} URLs... (Total visited: {len(visited_urls)})")
        all_discovered_links.update(current_batch)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(worker_fetch_links, url, config, session): url for url in current_batch}
            
            for future in as_completed(future_to_url):
                try:
                    newly_found_links = future.result()
                    with lock:
                        urls_to_visit.update(newly_found_links)
                except Exception as exc:
                    print(f'[!] An exception occurred in a worker future: {exc}', file=sys.stderr)

    if not all_discovered_links:
        print("[*] No URLs were discovered.")
        db_session.close()
        return

    print(f"\n[*] Discovered {len(all_discovered_links)} total unique URLs. Upserting to local queue...")
    
    try:
        links_list = [{"url": link} for link in all_discovered_links]
        for i in range(0, len(links_list), db_write_batch_size):
            chunk = links_list[i:i + db_write_batch_size]
            stmt = insert(CrawlQueue).values(chunk).on_conflict_do_nothing(index_elements=['url'])
            db_session.execute(stmt)
        
        db_session.commit()
        print("  [+] Upserted links to local crawl_queue.")
    except Exception as e:
        print(f"  [!] DB Error during final upsert: {e}", file=sys.stderr)
        db_session.rollback()
    finally:
        db_session.close()

    print(f"\n--- URL Discovery Finished ---")

if __name__ == "__main__":
    main()
