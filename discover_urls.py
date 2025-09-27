# discover_urls.py
import os
import sys
import requests
import hashlib
import configparser
import threading
import time
import random
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from supabase import create_client, Client
# 自動リトライのために追加
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- 共有リソースとロック ---
urls_to_visit = set()
visited_urls = set()
lock = threading.Lock()

def get_content_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()

def worker_discover_url(url: str, supabase_client, config, session):
    """単一URLを検証し、リンクを抽出し、キューに追加するワーカー"""
    global urls_to_visit, visited_urls
    
    target_domain = config.get('General', 'TARGET_DOMAIN')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    
    found_links = set()
    try:
        # print(f"[*] 検証中: {url}")
        
        # ▼▼▼ ステップ2: リクエスト間にランダムな遅延を入れる ▼▼▼
        time.sleep(random.uniform(0.5, 1.5))

        # ▼▼▼ ステップ3: sessionオブジェクトを使ってリクエスト（自動リトライ付き） ▼▼▼
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
            else:
                if db_res.data[0].get("content_hash") != new_hash:
                    supabase_client.table("crawl_queue").update({"status": "queued", "content_hash": new_hash}).eq("url", url).execute()

            soup = BeautifulSoup(response.content, 'html.parser')
            for a_tag in soup.find_all('a', href=True):
                link = urljoin(url, a_tag['href']).split('#')[0]
                if urlparse(link).netloc == target_domain:
                    found_links.add(link)
    
    except requests.exceptions.RequestException as req_e:
        # 404エラーなどはここで捕捉される
        print(f"  [!] リクエストエラー: {url} - {req_e}")
    except Exception as e:
        print(f"  [!] 不明なエラー: {url} - {e}")
    
    return found_links

def main():
    global urls_to_visit, visited_urls
    
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    start_url = config.get('General', 'START_URL')
    max_urls = config.getint('Discoverer', 'MAX_URLS_TO_DISCOVER')
    max_workers = config.getint('Discoverer', 'MAX_DISCOVER_WORKERS') # ステップ1

    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: raise ValueError("環境変数を設定してください。")
    supabase = create_client(supabase_url, supabase_key)
    
    print("--- URL高速発見処理開始 ---")

    # ▼▼▼ ステップ3: 自動リトライ機能を持つSessionオブジェクトを作成 ▼▼▼
    session = requests.Session()
    # サーバーエラー(5xx)が起きた場合に、最大3回まで、1秒->2秒->4秒の間隔でリトライ
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.mount('http://', HTTPAdapter(max_retries=retries))

    urls_to_visit.add(start_url)
    
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
