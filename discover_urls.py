# discover_urls.py
import os
import requests
import hashlib
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from supabase import create_client, Client

# --- 設定項目 ---
START_URL = "https://www.mhlw.go.jp/"
TARGET_DOMAIN = "www.mhlw.go.jp"
MAX_URLS_TO_DISCOVER = 500 # 0で上限なし
REQUEST_TIMEOUT = 15

def get_content_hash(content: bytes) -> str:
    """コンテンツのSHA256ハッシュ値を計算する"""
    return hashlib.sha256(content).hexdigest()

def main():
    """サイトをクロールし、変更があったURLのみをキューに追加する"""
    supabase_url: str = os.environ.get("SUPABASE_URL")
    supabase_key: str = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise ValueError("環境変数を設定してください。")

    supabase: Client = create_client(supabase_url, supabase_key)
    print("--- URL差分検知・発見処理開始 ---")

    urls_to_visit = {START_URL}
    visited_urls = set()
    
    while urls_to_visit and (MAX_URLS_TO_DISCOVER == 0 or len(visited_urls) < MAX_URLS_TO_DISCOVER):
        url = urls_to_visit.pop()
        if url in visited_urls:
            continue

        print(f"[*] 検証中: {url}")
        visited_urls.add(url)

        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers, allow_redirects=True)
            response.raise_for_status()
            
            new_hash = get_content_hash(response.content)

            # DBから既存のURL情報を取得
            db_res = supabase.table("crawl_queue").select("content_hash").eq("url", url).maybe_single().execute()
            
            # 応答がNoneの場合、このURLの処理をスキップして次に進む
            if db_res is None:
                print(f"  [!] DBからの応答がありませんでした。このURLをスキップします。")
                continue

            # DBにURLが存在しない (新規URL)
            if db_res.data is None:
                print(f"  [+] 新規URL発見。キューに追加します。")
                supabase.table("crawl_queue").insert({
                    "url": url,
                    "status": "queued",
                    "content_hash": new_hash
                }).execute()
            # DBにURLが存在する (既存URL)
            else:
                old_hash = db_res.data.get("content_hash")
                if old_hash != new_hash:
                    print(f"  [+] コンテンツ更新を検知。キューに再追加します。")
                    supabase.table("crawl_queue").update({
                        "status": "queued",
                        "content_hash": new_hash
                    }).eq("url", url).execute()

            # HTMLページからのみリンクを辿る
            content_type = response.headers.get("content_type", "").lower()
            if "html" in content_type:
                soup = BeautifulSoup(response.content, 'html.parser')
                for a_tag in soup.find_all('a', href=True):
                    link = urljoin(url, a_tag['href']).split('#')[0]
                    if urlparse(link).netloc == TARGET_DOMAIN and link not in visited_urls:
                        urls_to_visit.add(link)

        except requests.RequestException as e:
            print(f"  [!] HTTPエラー: {url} - {e}")
        except Exception as e:
            print(f"  [!] 不明なエラー: {url} - {e}")

    print(f"\n--- URL差分検知・発見処理終了 ---")
    print(f"今回訪問したURL数: {len(visited_urls)}")

if __name__ == "__main__":
    main()
