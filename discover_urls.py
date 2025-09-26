# discover_urls.py
import os
import sys
import requests
import hashlib
import configparser # configparserをインポート
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from supabase import create_client, Client

def get_content_hash(content: bytes) -> str:
    """コンテンツのSHA256ハッシュ値を計算する"""
    return hashlib.sha256(content).hexdigest()

def main():
    """サイトをクロールし、変更があったURLのみをキューに追加する"""
    # --- 設定ファイル読み込み ---
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    start_url = config.get('General', 'START_URL')
    target_domain = config.get('General', 'TARGET_DOMAIN')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    max_urls_to_discover = config.getint('Discoverer', 'MAX_URLS_TO_DISCOVER')
    
    # --- Supabaseクライアント初期化 ---
    supabase_url: str = os.environ.get("SUPABASE_URL")
    supabase_key: str = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise ValueError("環境変数を設定してください。")

    supabase: Client = create_client(supabase_url, supabase_key)
    print("--- URL差分検知・発見処理開始 ---")

    # DB接続テスト
    print("[*] Supabaseへの接続をテストしています...")
    try:
        test_res = supabase.table("stop_words").select("id", count='exact').limit(0).execute()
        if test_res is None or not hasattr(test_res, 'count'):
            raise ConnectionError("DBからの応答が不正です。")
        print(f"  [+] 接続成功。 (Stop Words: {test_res.count}件)")
    except Exception as e:
        print("\n[!!!] 致命的なエラー: Supabaseデータベースに接続できません。", file=sys.stderr)
        print(f"      - 詳細エラー: {e}", file=sys.stderr)
        sys.exit(1)

    urls_to_visit = {start_url}
    visited_urls = set()
    
    while urls_to_visit and (max_urls_to_discover == 0 or len(visited_urls) < max_urls_to_discover):
        url = urls_to_visit.pop()
        if url in visited_urls:
            continue

        print(f"[*] 検証中: {url}")
        visited_urls.add(url)

        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, timeout=request_timeout, headers=headers, allow_redirects=True)
            response.raise_for_status()
            
            new_hash = get_content_hash(response.content)

            db_res = supabase.table("crawl_queue").select("content_hash").eq("url", url).execute()

            if not hasattr(db_res, 'data'):
                print(f"  [!] DBからの応答が不正でした。このURLをスキップします。")
                continue

            if not db_res.data:
                print(f"  [+] 新規URL発見。キューに追加します。")
                supabase.table("crawl_queue").insert({
                    "url": url, "status": "queued", "content_hash": new_hash
                }).execute()
            else:
                old_hash = db_res.data[0].get("content_hash")
                if old_hash != new_hash:
                    print(f"  [+] コンテンツ更新を検知。キューに再追加します。")
                    supabase.table("crawl_queue").update({
                        "status": "queued", "content_hash": new_hash
                    }).eq("url", url).execute()

            content_type = response.headers.get("content-type", "").lower()
            if "html" in content_type:
                soup = BeautifulSoup(response.content, 'html.parser')
                for a_tag in soup.find_all('a', href=True):
                    link = urljoin(url, a_tag['href']).split('#')[0]
                    if urlparse(link).netloc == target_domain and link not in visited_urls:
                        urls_to_visit.add(link)

        except requests.RequestException as e:
            print(f"  [!] HTTPエラー: {url} - {e}")
        except Exception as e:
            print(f"  [!] 不明なエラー: {url} - {e}")

    print(f"\n--- URL差分検知・発見処理終了 ---")
    print(f"今回訪問したURL数: {len(visited_urls)}")

if __name__ == "__main__":
    main()
