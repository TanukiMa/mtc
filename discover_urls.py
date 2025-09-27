# discover_urls.py
import os
import sys
import requests
import hashlib
import configparser
import threading
import time  # timeモジュールをインポート
import random  # randomモジュールをインポート
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from supabase import create_client, Client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_content_hash(content: bytes) -> str:
    """コンテンツのSHA256ハッシュ値を計算する"""
    return hashlib.sha256(content).hexdigest()

def intelligent_worker(url_item: dict, supabase_client, config, session):
    """
    HEADリクエストで事前チェックし、変更があった場合のみ詳細な探索を行う賢いワーカー
    """
    url = url_item['url']
    old_last_modified = url_item.get('last_modified')
    target_domain = config.get('General', 'TARGET_DOMAIN')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    
    found_links = set()

    try:
        # ステップ1: まずは軽量なHEADリクエストでヘッダー情報のみ取得
        head_response = session.head(url, timeout=request_timeout, headers={'User-Agent': 'Mozilla/5.0'}, allow_redirects=True)
        head_response.raise_for_status()
        new_last_modified = head_response.headers.get('Last-Modified')

        # ステップ2: 最終更新日が前回と同じなら、ここで処理を終了
        if old_last_modified and new_last_modified and old_last_modified == new_last_modified:
            return set()

        # ステップ3: 変更があったか、新規URLの場合のみ、GETリクエストで本体を取得
        time.sleep(random.uniform(0.5, 1.0))
        response = session.get(url, timeout=request_timeout, headers={'User-Agent': 'Mozilla/5.0'}, allow_redirects=True)
        response.raise_for_status()
        
        content_type = response.headers.get("content-type", "").lower()

        if "html" in content_type:
            new_hash = get_content_hash(response.content)

            db_res = supabase_client.table("crawl_queue").select("content_hash").eq("url", url).single().execute()
            old_hash = db_res.data.get("content_hash") if db_res.data else None

            if old_hash != new_hash:
                # print(f"  [+] コンテンツ更新を検知 -> キュー追加: {url}")
                supabase_client.table("crawl_queue").update({
                    "status": "queued",
                    "content_hash": new_hash,
                    "last_modified": new_last_modified
                }).eq("url", url).execute()

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
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    sitemap_url = config.get('Seeds', 'SITEMAP_URL', fallback=None)
    index_pages = config.get('Seeds', 'INDEX_PAGES').strip().split('\n')
    max_workers = config.getint('Discoverer', 'MAX_DISCOVER_WORKERS')

    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: raise ValueError("環境変数を設定してください。")
    supabase = create_client(supabase_url, supabase_key)
    
    print("--- ハイブリッド型・差分クロール開始 ---")

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    print("[*] DBから既知のURLリストを取得しています...")
    try:
        response = supabase.table("crawl_queue").select("id, url, last_modified").execute()
        all_known_urls = response.data
        print(f"  [+] {len(all_known_urls)}件の既知URLを取得しました。")
    except Exception as e:
        print(f"  [!] 既知URLの取得に失敗: {e}")
        all_known_urls = []

    target_urls = {item['url']: item for item in all_known_urls}
    
    seed_urls = set(filter(None, index_pages))
    if sitemap_url:
        seed_urls.update(parse_sitemap(sitemap_url, session))

    for page in seed_urls:
        if page not in target_urls:
            target_urls[page] = {"url": page}

    target_items = list(target_urls.values())
    print(f"[*] 合計 {len(target_items)} 件のURLを検証します。")

    newly_discovered_links = set()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(intelligent_worker, item, supabase, config, session): item['url'] for item in target_items}
        
        for future in as_completed(future_to_url):
            try:
                newly_discovered_links.update(future.result())
            except Exception as exc:
                print(f'[!] ワーカーで例外が発生しました: {exc}')

    if newly_discovered_links:
        response = supabase.table("crawl_queue").select("url").in_("url", list(newly_discovered_links)).execute()
        existing_urls_in_db = {item['url'] for item in response.data}
        
        truly_new_urls = newly_discovered_links - existing_urls_in_db
        
        if truly_new_urls:
            print(f"[*] {len(truly_new_urls)}件の全く新しいURLをキューに追加します。")
            supabase.table("crawl_queue").upsert(
                [{"url": link, "status": "queued"} for link in truly_new_urls], 
                on_conflict="url"
            ).execute()

    print(f"\n--- 差分クロール終了 ---")

if __name__ == "__main__":
    main()
