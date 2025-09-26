# discover_urls.py
import os
import sys
import requests
import hashlib
import configparser
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from supabase import create_client, Client

def get_content_hash(content: bytes) -> str:
    """コンテンツのSHA256ハッシュ値を計算する"""
    return hashlib.sha256(content).hexdigest()

def worker_discover_url(url: str, supabase_client, config):
    """
    単一URLを検証し、キューに追加し、新しいリンクを返すワーカー。
    この関数はメインプロセスではなく、スレッドで実行される。
    """
    target_domain = config.get('General', 'TARGET_DOMAIN')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    
    found_links = set()
    try:
        # print(f"[*] 検証中: {url}") # ログが多すぎる場合はコメントアウト
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, timeout=request_timeout, headers=headers, allow_redirects=True)
        response.raise_for_status()
        
        content_type = response.headers.get("content-type", "").lower()

        if "html" in content_type:
            new_hash = get_content_hash(response.content)

            db_res = supabase_client.table("crawl_queue").select("content_hash").eq("url", url).execute()
            if not hasattr(db_res, 'data'):
                print(f"  [!] DB応答不正: {url}")
                return found_links

            if not db_res.data:
                # print(f"  [+] 新規発見 -> キュー追加: {url}")
                supabase_client.table("crawl_queue").insert({"url": url, "status": "queued", "content_hash": new_hash}).execute()
            else:
                if db_res.data[0].get("content_hash") != new_hash:
                    # print(f"  [+] 更新検知 -> キュー追加: {url}")
                    supabase_client.table("crawl_queue").update({"status": "queued", "content_hash": new_hash}).eq("url", url).execute()

            soup = BeautifulSoup(response.content, 'html.parser')
            for a_tag in soup.find_all('a', href=True):
                link = urljoin(url, a_tag['href']).split('#')[0]
                if urlparse(link).netloc == target_domain:
                    found_links.add(link)
    except Exception as e:
        print(f"  [!] エラー: {url} - {e}")
    
    return found_links

def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    start_url = config.get('General', 'START_URL')
    max_urls = config.getint('Discoverer', 'MAX_URLS_TO_DISCOVER')
    max_workers = config.getint('Discoverer', 'MAX_DISCOVER_WORKERS')

    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: raise ValueError("環境変数を設定してください。")
    supabase = create_client(supabase_url, supabase_key)
    
    print("--- URL高速発見処理開始 ---")

    urls_to_visit = {start_url}
    visited_urls = set()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while urls_to_visit:
            if max_urls != 0 and len(visited_urls) >= max_urls:
                print(f"[*] 上限 {max_urls} URLに達したため、探索を停止します。")
                break

            # 現在の階層で訪問すべきURLのリストを作成
            current_batch = list(urls_to_visit)
            # 訪問済みリストに追加
            visited_urls.update(current_batch)
            # 次の訪問先リストをクリア
            urls_to_visit.clear()

            print(f"[*] {len(current_batch)}件のURLを並列で検証します... (累計: {len(visited_urls)}件)")

            # バッチ処理をワーカーに投入
            future_to_url = {executor.submit(worker_discover_url, url, supabase, config): url for url in current_batch}
            
            # バッチ内の全ワーカーの処理完了を待つ
            for future in as_completed(future_to_url):
                try:
                    # ワーカーが発見した新しいリンクを取得
                    newly_found_links = future.result()
                    # まだ訪問していないURLだけを次の訪問先リストに追加
                    urls_to_visit.update(newly_found_links - visited_urls)
                except Exception as exc:
                    print(f'[!] ワーカーで例外が発生しました: {exc}')

    print(f"\n--- URL高速発見処理終了 ---")
    print(f"今回訪問したURL数: {len(visited_urls)}")

if __name__ == "__main__":
    main()
