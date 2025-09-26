# discover_urls.py
import os
import sys
import requests
import hashlib
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

def get_content_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()

def worker_discover_url(url: str, supabase_client, config):
    """単一URLを検証し、リンクを抽出し、キューに追加するワーカー"""
    global urls_to_visit, visited_urls
    
    target_domain = config.get('General', 'TARGET_DOMAIN')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    
    try:
        print(f"[*] 検証中: {url}")
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, timeout=request_timeout, headers=headers, allow_redirects=True)
        response.raise_for_status()
        
        content_type = response.headers.get("content-type", "").lower()

        if "html" in content_type:
            new_hash = get_content_hash(response.content)

            db_res = supabase_client.table("crawl_queue").select("content_hash").eq("url", url).execute()
            if not hasattr(db_res, 'data'):
                print(f"  [!] DB応答不正: {url}")
                return

            if not db_res.data:
                print(f"  [+] 新規発見 -> キュー追加: {url}")
                supabase_client.table("crawl_queue").insert({"url": url, "status": "queued", "content_hash": new_hash}).execute()
            else:
                if db_res.data[0].get("content_hash") != new_hash:
                    print(f"  [+] 更新検知 -> キュー追加: {url}")
                    supabase_client.table("crawl_queue").update({"status": "queued", "content_hash": new_hash}).eq("url", url).execute()

            soup = BeautifulSoup(response.content, 'html.parser')
            new_links = set()
            for a_tag in soup.find_all('a', href=True):
                link = urljoin(url, a_tag['href']).split('#')[0]
                if urlparse(link).netloc == target_domain:
                    new_links.add(link)
            
            # 共有リソースへのアクセスをロック
            with lock:
                urls_to_visit.update(new_links - visited_urls)
        else:
            print(f"  [-] HTMLでないためスキップ: {url}")

    except Exception as e:
        print(f"  [!] エラー: {url} - {e}")

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
