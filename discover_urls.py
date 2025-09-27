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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- 共有リソースとロック ---
urls_to_visit = set()
visited_urls = set()
lock = threading.Lock()

# --- ワーカー関数とヘルパー関数 (変更なし) ---
def worker_discover_url(url: str, config, session):
    """単一URLからリンクを発見し、新しいリンクのセット等を返すワーカー"""
    # ... (この関数の中身は変更ありません) ...
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
            soup = BeautifulSoup(response.content, 'html.parser')
            for a_tag in soup.find_all('a', href=True):
                link = urljoin(url, a_tag['href']).split('#')[0]
                if urlparse(link).netloc == target_domain:
                    found_links.add(link)
    except requests.exceptions.RequestException as req_e:
        print(f"  [!] リクエストエラー: {url} - {req_e}")
    except Exception as e:
        print(f"  [!] 不明なエラー: {url} - {e}")
    return url, response.content if 'response' in locals() and response.ok else None, found_links


def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    start_url = config.get('General', 'START_URL')
    max_urls = config.getint('Discoverer', 'MAX_URLS_TO_DISCOVER')
    max_workers = config.getint('Discoverer', 'MAX_DISCOVER_WORKERS')
    db_write_batch_size = config.getint('Discoverer', 'DB_WRITE_BATCH_SIZE')

    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: raise ValueError("環境変数を設定してください。")
    supabase = create_client(supabase_url, supabase_key)
    
    print("--- URL高速発見処理開始 ---")

    session = requests.Session()
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

            future_to_url = {executor.submit(worker_discover_url, url, config, session): url for url in current_batch}
            
            # ▼▼▼▼▼ ここからが修正箇所 ▼▼▼▼▼
            newly_found_links_total = set()
            urls_to_queue_chunk = []

            for future in as_completed(future_to_url):
                try:
                    original_url, content, newly_found_links = future.result()
                    
                    if content:
                        new_hash = hashlib.sha256(content).hexdigest()
                        db_res = supabase.table("crawl_queue").select("content_hash").eq("url", original_url).execute()
                        if not hasattr(db_res, 'data'): continue
                        if not db_res.data:
                            urls_to_queue_chunk.append({"url": original_url, "status": "queued", "content_hash": new_hash})
                        elif db_res.data[0].get("content_hash") != new_hash:
                            urls_to_queue_chunk.append({"url": original_url, "status": "queued", "content_hash": new_hash})

                    newly_found_links_total.update(newly_found_links)

                    # チャンクサイズに達したら、DBに書き込んでチャンクをクリア
                    if len(urls_to_queue_chunk) >= db_write_batch_size:
                        print(f"  [+] {len(urls_to_queue_chunk)}件のURLをキューに一括追加します。")
                        supabase.table("crawl_queue").upsert(urls_to_queue_chunk, on_conflict="url").execute()
                        urls_to_queue_chunk.clear()

                except Exception as exc:
                    print(f'[!] ワーカーの結果集約で例外が発生しました: {exc}')
            
            # ループ終了後、チャンクに残っているURLを書き込む
            if urls_to_queue_chunk:
                print(f"  [+] 残りの{len(urls_to_queue_chunk)}件のURLをキューに一括追加します。")
                supabase.table("crawl_queue").upsert(urls_to_queue_chunk, on_conflict="url").execute()
                urls_to_queue_chunk.clear()

            with lock:
                urls_to_visit.update(newly_found_links_total - visited_urls)
            # ▲▲▲▲▲ ここまで修正 ▲▲▲▲▲

    print(f"\n--- URL高速発見処理終了 ---")
    print(f"今回訪問したURL数: {len(visited_urls)}")

if __name__ == "__main__":
    main()
