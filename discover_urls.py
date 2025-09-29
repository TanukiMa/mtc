# discover_urls.py
import os
import sys
import requests
import configparser
import time
import random
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from supabase import create_client, Client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from url_normalize import url_normalize

def fetch_links_from_url(url: str, config, session) -> set:
    """単一のURLからリンクをすべて抽出し、正規化してセットとして返す"""
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
                except Exception:
                    pass
    except Exception as e:
        print(f"  [!] エラー: {url} - {e}", file=sys.stderr)
    
    return found_links

def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    # config.iniから設定を読み込む
    index_pages = list(filter(None, config.get('Seeds', 'INDEX_PAGES').strip().split('\n')))
    max_workers = config.getint('Discoverer', 'MAX_DISCOVER_WORKERS')
    crawl_depth = config.getint('Discoverer', 'CRAWL_DEPTH')

    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: raise ValueError("環境変数を設定してください。")
    supabase = create_client(supabase_url, supabase_key)
    
    print(f"--- 「多階層クロール(深さ: {crawl_depth})」によるURL発見処理開始 ---")

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    # --- 階層的クロールのメインロジック ---
    all_discovered_links = set()
    visited_urls = set()
    # 最初の階層(depth 0)はインデックスページ
    urls_for_next_level = set(url_normalize(url) for url in index_pages)

    for depth in range(crawl_depth):
        current_level_urls = urls_for_next_level - visited_urls
        if not current_level_urls:
            print(f"[*] 深さ {depth + 1}: 新しい探索対象URLがないため終了します。")
            break

        print(f"\n[*] 深さ {depth + 1}/{crawl_depth}: {len(current_level_urls)}件のURLを検証します...")
        
        visited_urls.update(current_level_urls)
        all_discovered_links.update(current_level_urls)
        urls_for_next_level.clear()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(fetch_links_from_url, url, config, session): url for url in current_level_urls}
            
            for future in as_completed(future_to_url):
                try:
                    # 次の階層で探索するURLを収集
                    urls_for_next_level.update(future.result())
                except Exception as exc:
                    print(f'[!] ワーカーで例外が発生しました: {exc}', file=sys.stderr)

    if not all_discovered_links:
        print("[*] 解析対象のURLは発見されませんでした。")
        return

    print(f"\n[*] 合計 {len(all_discovered_links)}件のユニークなURLを発見しました。キューに追加します...")
    
    try:
        chunk_size = 500
        links_list = list(all_discovered_links)
        for i in range(0, len(links_list), chunk_size):
            chunk = links_list[i:i + chunk_size]
            supabase.table("crawl_queue").upsert(
                [{"url": link, "status": "queued"} for link in chunk], 
                on_conflict="url"
            ).execute()
        print("  [+] キューへの追加が完了しました。")
    except Exception as e:
        print(f"  [!] キューへの一括書き込みでエラー: {e}", file=sys.stderr)

    print(f"\n--- URL発見処理終了 ---")

if __name__ == "__main__":
    main()
