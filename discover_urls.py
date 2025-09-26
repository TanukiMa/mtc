# discover_urls.py
import os
import sys
import requests
import configparser
import threading
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from supabase import create_client, Client

# --- 共有リソースとロック ---
urls_to_visit = set()
visited_urls = set()
lock = threading.Lock()

def worker_discover_url(url: str, supabase_client, config):
    """単一URLからリンクを抽出し、発見したURLをキューと次の訪問先に追加する"""
    global urls_to_visit, visited_urls
    
    target_domain = config.get('General', 'TARGET_DOMAIN')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    
    try:
        # print(f"[*] 探索中: {url}")
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, timeout=request_timeout, headers=headers, allow_redirects=True)
        response.raise_for_status()
        
        # 自身のURLをまずキューに追加（存在すれば無視される）
        supabase_client.table("crawl_queue").upsert({"url": url}, on_conflict="url").execute()

        # HTMLページからのみリンクを探索
        content_type = response.headers.get("content-type", "").lower()
        if "html" in content_type:
            soup = BeautifulSoup(response.content, 'html.parser')
            new_links = set()
            for a_tag in soup.find_all('a', href=True):
                link = urljoin(url, a_tag['href']).split('#')[0]
                if urlparse(link).netloc == target_domain:
                    new_links.add(link)
            
            # 共有リソースへのアクセスをロック
            with lock:
                urls_to_visit.update(new_links - visited_urls)

    except requests.RequestException:
        # 接続エラーなどは静かに無視して次に進む
        pass
    except Exception as e:
        print(f"  [!] 発見処理中の不明なエラー: {url} - {e}")

def main():
    global urls_to_visit, visited_urls
    
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    start_url = config.get('General', 'START_URL')
    max_urls = config.getint('Discoverer', 'MAX_URLS_TO_DISCOVER')
    max_workers = config.getint('Discoverer', 'MAX_DISCOVER_WORKERS')

    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: raise ValueError("環境変数を設定してください。")
    supabase = create_client(supabase_url, supabase_key)
    
    print("--- URL高速発見処理開始 ---")

    urls_to_visit.add(start_url)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while urls_to_visit:
            if max_urls != 0 and len(visited_urls) >= max_urls:
                print(f"[*] 上限 {max_urls} に達したため、処理を停止します。")
                break

            with lock:
                url = urls_to_visit.pop()
                if url in visited_urls:
                    continue
                visited_urls.add(url)
            
            executor.submit(worker_discover_url, url, supabase, config)

    print(f"\n--- URL高速発見処理終了 ---")
    print(f"今回訪問したURL数: {len(visited_urls)}")

if __name__ == "__main__":
    main()
