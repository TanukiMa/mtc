# discover_urls.py
import os
import sys
import requests
import hashlib
import configparser
import threading
import time
import random
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from supabase import create_client, Client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- 共有リソースとロック ---
urls_to_visit = set()
visited_urls = set()
lock = threading.Lock()

def get_content_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()

def worker_discover_url(url: str, supabase_client, config, session):
    global urls_to_visit, visited_urls
    target_domain = config.get('General', 'TARGET_DOMAIN')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    found_links = set()
    try:
        time.sleep(random.uniform(0.5, 1.5))
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = session.get(url, timeout=request_timeout, headers=headers, allow_redirects=True)
        response.raise_for_status()
        content_type = response.headers.get("content-type", "").lower()

        if "html" in content_type:
            new_hash = get_content_hash(response.content)
            db_res = supabase_client.table("crawl_queue").select("content_hash").eq("url", url).execute()
            if not hasattr(db_res, 'data'):
                print(f"  [!] DB応答不正: {url}")
                return found_links

            if not db_res.data:
                supabase_client.table("crawl_queue").insert({"url": url, "status": "queued", "content_hash": new_hash}).execute()
                print(f"  [+] キュー追加 (新規): {url}")
            else:
                if db_res.data[0].get("content_hash") != new_hash:
                    supabase_client.table("crawl_queue").update({"status": "queued", "content_hash": new_hash}).eq("url", url).execute()
                    print(f"  [+] キュー追加 (更新): {url}")

            soup = BeautifulSoup(response.content, 'html.parser')
            for a_tag in soup.find_all('a', href=True):
                link = urljoin(url, a_tag['href']).split('#')[0]
                if urlparse(link).netloc == target_domain:
                    found_links.add(link)
    except requests.exceptions.RequestException as req_e:
        print(f"  [!] リクエストエラー: {url} - {req_e}")
    except Exception as e:
        print(f"  [!] 不明なエラー: {url} - {e}")
    return found_links

def parse_sitemap(sitemap_url: str, session) -> set:
    """サイトマップXMLを解析し、URLのセットを返す"""
    print(f"[*] サイトマップを解析中: {sitemap_url}")
    try:
        response = session.get(sitemap_url, timeout=30)
        response.raise_for_status()
        namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        root = ET.fromstring(response.content)
        locs = [loc.text for loc in root.findall('ns:url/ns:loc', namespaces)]
        print(f"  [+] サイトマップから {len(locs)} 件のURLを発見しました。")
        return set(locs)
    except Exception as e:
        print(f"  [!] サイトマップの解析に失敗しました: {e}")
        return set()

def main():
    global urls_to_visit, visited_urls
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    sitemap_url = config.get('Seeds', 'SITEMAP_URL', fallback=None)
    index_pages = config.get('Seeds', 'INDEX_PAGES').strip().split('\n')
    max_urls = config.getint('Discoverer', 'MAX_URLS_TO_DISCOVER')
    max_workers = config.getint('Discoverer', 'MAX_DISCOVER_WORKERS')

    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: raise ValueError("環境変数を設定してください。")
    supabase = create_client(supabase_url, supabase_key)
    
    print("--- URL高速発見処理開始 ---")

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.mount('http://', HTTPAdapter(max_retries=retries))

    initial_seeds = set(filter(None, index_pages)) # 空行を除去
    # SITEMAP_URLがconfig.iniに存在し、Noneでない場合のみ解析を実行
    if sitemap_url:
        initial_seeds.update(parse_sitemap(sitemap_url, session))
    
    urls_to_visit.update(initial_seeds)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while urls_to_visit:
            if max_urls != 0 and len(visited_urls) >= max_urls:
                print(f"[*] 上限 {max_urls} URLに達したため、探索を停止します。")
                break
            with lock:
                current_batch = list(urls_to_visit - visited_urls)
                visited_urls.update(current_batch)
                urls_to_visit.clear()
            if not current_batch:
                break
            print(f"[*] {len(current_batch)}件のURLを並列で検証します... (累計: {len(visited_urls)}件)")
            future_to_url = {executor.submit(worker_discover_url, url, supabase, config, session): url for url in current_batch}
            for future in as_completed(future_to_url):
                try:
                    newly_found_links = future.result()
                    with lock:
                        urls_to_visit.update(newly_found_links - visited_urls)
                except Exception as exc:
                    print(f'[!] ワーカーで例外が発生しました: {exc}')

    print(f"\n--- URL高速発見処理終了 ---")
    print(f"今回訪問したURL数: {len(visited_urls)}")

if __name__ == "__main__":
    main()
