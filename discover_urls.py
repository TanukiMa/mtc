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

def get_content_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()

def intelligent_worker(url_item: dict, supabase_client, config, session):
    """HEADリクエストで事前チェックし、変更があった場合のみ詳細な探索を行う賢いワーカー"""
    url = url_item['url']
    old_last_modified = url_item.get('last_modified')
    target_domain = config.get('General', 'TARGET_DOMAIN')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    
    found_links = set()
    update_payload = None

    try:
        time.sleep(random.uniform(0.5, 1.0))
        head_response = session.head(url, timeout=request_timeout, headers={'User-Agent': 'Mozilla/5.0'}, allow_redirects=True)
        head_response.raise_for_status()
        new_last_modified = head_response.headers.get('Last-Modified')

        if old_last_modified and new_last_modified and old_last_modified == new_last_modified:
            return url, None, set()

        response = session.get(url, timeout=request_timeout, headers={'User-Agent': 'Mozilla/5.0'}, allow_redirects=True)
        response.raise_for_status()
        
        content_type = response.headers.get("content-type", "").lower()

        if "html" in content_type:
            new_hash = get_content_hash(response.content)

            db_res = supabase_client.table("crawl_queue").select("content_hash").eq("url", url).maybe_single().execute()
            old_hash = db_res.data.get("content_hash") if db_res.data else None

            if old_hash != new_hash:
                update_payload = {
                    "url": url,
                    "status": "queued",
                    "content_hash": new_hash,
                    "last_modified": new_last_modified
                }

            soup = BeautifulSoup(response.content, 'html.parser')
            for a_tag in soup.find_all('a', href=True):
                link = urljoin(url, a_tag['href']).split('#')[0]
                if urlparse(link).netloc == target_domain:
                    found_links.add(link)

    except requests.exceptions.RequestException:
        pass
    except Exception as e:
        print(f"  [!] 不明なエラー: {url} - {e}")
    
    return url, update_payload, found_links

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
        try:
            sitemap_links = parse_sitemap(sitemap_url, session)
            seed_urls.update(sitemap_links)
        except Exception as e:
            print(f"  [!] サイトマップ解析中にエラー: {e}")

    for page in seed_urls:
        if page not in target_urls:
            target_urls[page] = {"url": page}

    target_items = list(target_urls.values())
    print(f"[*] 合計 {len(target_items)} 件のURLを検証します。")

    newly_discovered_links = set()
    urls_to_update_queue = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(intelligent_worker, item, supabase, config, session): item['url'] for item in target_items}
        
        for future in as_completed(future_to_url):
            try:
                original_url, update_payload, found_links = future.result()
                if update_payload:
                    urls_to_update_queue.append(update_payload)
                newly_discovered_links.update(found_links)
            except Exception as exc:
                print(f'[!] ワーカーで例外が発生しました: {exc}')

    # 更新が必要なURLを一括でキューに追加
    if urls_to_update_queue:
        print(f"[*] {len(urls_to_update_queue)}件の更新されたURLをキューに追加します。")
        supabase.table("crawl_queue").upsert(urls_to_update_queue, on_conflict="url").execute()

    # 全く新しいURLをチャンクに分けてキューに追加
    if newly_discovered_links:
        print(f"[*] 発見した {len(newly_discovered_links)} 件のリンクから、新規URLをチェックします...")
        
        # ▼▼▼▼▼ ここからが修正箇所 ▼▼▼▼▼
        # 全リンクをチャンクに分割
        all_links_list = list(newly_discovered_links)
        chunk_size = 10 # 一度にDBに問い合わせるURL数
        truly_new_urls = []

        for i in range(0, len(all_links_list), chunk_size):
            chunk = all_links_list[i:i + chunk_size]
            
            # DBに既に存在するURLをチャンクごとに問い合わせ
            response = supabase.table("crawl_queue").select("url").in_("url", chunk).execute()
            existing_urls_in_chunk = {item['url'] for item in response.data}
            
            # チャンク内で新規のものだけをリストに追加
            truly_new_urls.extend([url for url in chunk if url not in existing_urls_in_chunk])

        if truly_new_urls:
            print(f"[*] {len(truly_new_urls)}件の全く新しいURLをキューに追加します。")
            # 新規URLもチャンクに分けてDBに登録
            for i in range(0, len(truly_new_urls), chunk_size):
                chunk = truly_new_urls[i:i + chunk_size]
                supabase.table("crawl_queue").upsert(
                    [{"url": link, "status": "queued"} for link in chunk], 
                    on_conflict="url"
                ).execute()
        # ▲▲▲▲▲ ここまで修正 ▲▲▲▲▲
        else:
            print("[*] 全く新しいURLは見つかりませんでした。")

    print(f"\n--- 差分クロール終了 ---")

# (parse_sitemap関数は前回のままで変更なし)
def parse_sitemap(sitemap_url: str, session) -> set:
    print(f"[*] サイトマップを解析中: {sitemap_url}")
    response = session.get(sitemap_url, timeout=30)
    response.raise_for_status()
    namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    root = ET.fromstring(response.content)
    locs = [loc.text for loc in root.findall('ns:url/ns:loc', namespaces)]
    print(f"  [+] サイトマップから {len(locs)} 件のURLを発見しました。")
    return set(locs)

if __name__ == "__main__":
    main()
