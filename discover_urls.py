# discover_urls.py
import os
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from supabase import create_client, Client

# --- 設定項目 ---
START_URL = "https://www.mhlw.go.jp/"
TARGET_DOMAIN = "www.mhlw.go.jp"
# 1回の実行で発見を試みるURLの上限 (0 を設定すると上限なし)
MAX_URLS_TO_DISCOVER = 500
REQUEST_TIMEOUT = 15

def main():
    """サイトをクロールし、発見したURLをキューに追加する"""
    supabase_url: str = os.environ.get("SUPABASE_URL")
    supabase_key: str = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise ValueError("環境変数 SUPABASE_URL と SUPABASE_KEY を設定してください。")

    supabase: Client = create_client(supabase_url, supabase_key)
    print("--- URL発見処理開始 ---")

    urls_to_visit = {START_URL}
    visited_urls = set()
    discovered_count = 0

    while urls_to_visit and (MAX_URLS_TO_DISCOVER == 0 or discovered_count < MAX_URLS_TO_DISCOVER):
        url = urls_to_visit.pop()
        if url in visited_urls:
            continue

        print(f"[*] 探索中: {url}")
        visited_urls.add(url)
        discovered_count += 1

        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            response = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers, allow_redirects=True)
            response.raise_for_status()

            # 発見したURLをキューに追加
            try:
                # 既にキューにあれば無視される
                supabase.table("crawl_queue").upsert({"url": url}, on_conflict="url").execute()
            except Exception as db_e:
                print(f"  [!] キューへの追加エラー: {db_e}")

            # HTMLページからのみリンクを辿る
            content_type = response.headers.get("content-type", "").lower()
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

    print(f"\n--- URL発見処理終了 ---")
    print(f"今回訪問したURL数: {discovered_count}")

if __name__ == "__main__":
    main()
