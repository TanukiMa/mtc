# discover_urls.py
import os
import sys
import requests
import configparser
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
    
    print(f"[*] インデックスページを検証中: {url}")
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
                    # 不正なURLは無視
                    pass
    except Exception as e:
        print(f"  [!] エラー: {url} - {e}", file=sys.stderr)
    
    return found_links

def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    index_pages = list(filter(None, config.get('Seeds', 'INDEX_PAGES').strip().split('\n')))
    max_workers = config.getint('Discoverer', 'MAX_DISCOVER_WORKERS')

    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: raise ValueError("環境変数を設定してください。")
    supabase = create_client(supabase_url, supabase_key)
    
    print("--- 「浅いクロール」によるURL発見処理開始 ---")

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.mount('http://', HTTPAdapter(max_retries=retries))

    all_discovered_links = set()
    
    # インデックスページ群を並列処理
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(fetch_links_from_url, url, config, session): url for url in index_pages}
        
        for future in as_completed(future_to_url):
            try:
                # 各インデックスページから見つかったリンクをすべて集約
                all_discovered_links.update(future.result())
            except Exception as exc:
                print(f'[!] ワーカーで例外が発生しました: {exc}', file=sys.stderr)

    if not all_discovered_links:
        print("[*] 解析対象のURLは発見されませんでした。")
        return

    print(f"[*] {len(all_discovered_links)}件のURLを発見しました。キューに追加します...")
    
    # 発見したURLをDBに一括登録 (ステータスは'queued'、ハッシュ値はまだ不明なのでnull)
    try:
        # チャンクに分割してupsert
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
