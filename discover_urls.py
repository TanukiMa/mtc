# discover_urls.py
import os
import sys
import requests
import configparser
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from supabase import create_client, Client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def worker_discover_url(url: str, supabase_url: str, supabase_key: str, config):
    """単一URLからリンクを発見し、見つけたURLをキューに追加する"""
    target_domain = config.get('General', 'TARGET_DOMAIN')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    supabase = create_client(supabase_url, supabase_key)
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = session.get(url, timeout=request_timeout, headers=headers, allow_redirects=True)
        response.raise_for_status()
        
        content_type = response.headers.get("content-type", "").lower()

        if "html" in content_type:
            soup = BeautifulSoup(response.content, 'html.parser')
            new_links = set()
            for a_tag in soup.find_all('a', href=True):
                link = urljoin(url, a_tag['href']).split('#')[0]
                if urlparse(link).netloc == target_domain:
                    new_links.add(link)
            
            # 発見したURLをDBに一括登録 (存在すれば無視される)
            if new_links:
                supabase.table("crawl_queue").upsert(
                    [{"url": link} for link in new_links], 
                    on_conflict="url"
                ).execute()
    except Exception as e:
        print(f"  [!] エラー: {url} - {e}")

def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    start_url = config.get('General', 'START_URL')
    max_workers = config.getint('Discoverer', 'MAX_DISCOVER_WORKERS')
    # 1回のループで処理するバッチサイズ
    discover_batch_size = 50 

    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: raise ValueError("環境変数を設定してください。")
    supabase_main = create_client(supabase_url, supabase_key)
    
    print("--- URL高速発見処理開始 (ステートレスモード) ---")

    # 最初の起点となるURLをキューに登録
    supabase_main.table("crawl_queue").upsert({"url": start_url}, on_conflict="url").execute()
    
    total_discovered_count = 0
    while True:
        # 1. DBから未発見のURLをバッチ取得し、'discovering'状態に更新
        response = supabase_main.rpc('get_and_lock_undiscovered_urls', {'limit_count': discover_batch_size}).execute()
        urls_to_discover = response.data
        
        if not urls_to_discover:
            print("[*] 発見対象のURLがキューにありません。終了します。")
            break
        
        batch_count = len(urls_to_discover)
        total_discovered_count += batch_count
        print(f"[*] {batch_count}件のURLからリンクを並列で発見します... (累計: {total_discovered_count}件)")
        
        # 2. 並列処理でリンクを発見
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(worker_discover_url, item['url'], supabase_url, supabase_key, config) for item in urls_to_discover]
            [f.result() for f in futures] # 処理完了を待つ

        # 3. 処理済みURLのステータスを'discovered'に更新
        discovered_ids = [item['id'] for item in urls_to_discover]
        supabase_main.table("crawl_queue").update({"discovery_status": "discovered"}).in_("id", discovered_ids).execute()

    print(f"\n--- URL高速発見処理終了 ---")
    print(f"今回リンク探索を試みたURL数: {total_discovered_count}")

if __name__ == "__main__":
    main()
