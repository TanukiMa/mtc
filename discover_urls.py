# discover_urls.py
import os, sys, requests, configparser
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from supabase import create_client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from url_normalize import url_normalize

def fetch_links_from_url(url: str, config, session) -> set:
    target_domain = config.get('General', 'TARGET_DOMAIN')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    found_links = set()
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = session.get(url, timeout=request_timeout, headers=headers, allow_redirects=True)
        response.raise_for_status()
        content_type = response.headers.get("content-type", "").lower()
        if "html" in content_type:
            soup = BeautifulSoup(response.content, 'html.parser')
            for a_tag in soup.find_all('a', href=True):
                try:
                    link = urljoin(url, a_tag['href'])
                    normalized_link = url_normalize(link)
                    if urlparse(normalized_link).netloc == target_domain:
                        found_links.add(normalized_link)
                except Exception: pass
    except Exception as e:
        print(f"  [!] Error discovering from {url}: {e}", file=sys.stderr)
    return found_links

def main():
    config = configparser.ConfigParser(); config.read('config.ini')
    index_pages = list(filter(None, config.get('Seeds', 'INDEX_PAGES').strip().split('\n')))
    max_workers = config.getint('Discoverer', 'MAX_DISCOVER_WORKERS')
    crawl_depth = config.getint('Discoverer', 'CRAWL_DEPTH')

    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    supabase = create_client(supabase_url, supabase_key)
    
    print(f"--- URL Discovery Started (Depth: {crawl_depth}) ---")

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504], , connect=3)
    session.mount('https://', HTTPAdapter(max_retries=retries))

    all_discovered_links = set()
    visited_urls = set()
    urls_for_next_level = set(url_normalize(url) for url in index_pages)

    for depth in range(crawl_depth):
        current_level_urls = urls_for_next_level - visited_urls
        if not current_level_urls:
            print(f"[*] Depth {depth + 1}: No new URLs to discover.")
            break

        print(f"[*] Depth {depth + 1}/{crawl_depth}: Discovering from {len(current_level_urls)} URLs...")
        visited_urls.update(current_level_urls)
        all_discovered_links.update(current_level_urls)
        urls_for_next_level.clear()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_links_from_url, url, config, session) for url in current_level_urls]
            for future in as_completed(futures):
                urls_for_next_level.update(future.result())

    if not all_discovered_links:
        print("[*] No URLs were discovered.")
        return

    print(f"\n[*] Discovered {len(all_discovered_links)} total unique URLs. Upserting to queue...")
    
    try:
        chunk_size = 500
        links_list = list(all_discovered_links)
        for i in range(0, len(links_list), chunk_size):
            chunk = links_list[i:i + chunk_size]
            supabase.table("crawl_queue").upsert([{"url": link} for link in chunk], on_conflict="url").execute()
        print("  [+] Upserted links to crawl_queue.")
    except Exception as e:
        print(f"  [!] DB Error during upsert: {e}", file=sys.stderr)

    print(f"\n--- URL Discovery Finished ---")

if __name__ == "__main__":
    main()
