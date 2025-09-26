# process_queue.py
import re
import os
import requests
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor

from bs4 import BeautifulSoup
from supabase import create_client, Client
from sudachipy import tokenizer, dictionary

# --- 設定項目 ---
MAX_WORKERS = 4
PROCESS_BATCH_SIZE = 50
REQUEST_TIMEOUT = 15

# --- Sudachi Tokenizerのためのグローバル変数 ---
_WORKER_TOKENIZER = None

# --- テキスト抽出・解析関連の関数群 ---
def clean_text(text: str) -> str:
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

def get_text_from_html(content: bytes) -> str:
    """HTMLコンテンツからテキストのみを抽出する"""
    try:
        soup = BeautifulSoup(content, 'html.parser')
        for s in soup(['script', 'style']): s.decompose()
        text = ' '.join(soup.stripped_strings)
        return clean_text(text)
    except Exception as e:
        raise RuntimeError(f"HTML解析エラー: {e}")

def analyze_with_sudachi(text: str, tokenizer_obj) -> list:
    if not text.strip() or not tokenizer_obj: return []
    chunk_size = 40000
    words = []
    for i in range(0, len(text), chunk_size):
        chunk = text[i:i + chunk_size]
        morphemes = tokenizer_obj.tokenize(chunk)
        for m in morphemes:
            pos_info = m.part_of_speech()
            if m.is_oov() and pos_info[0] == "名詞" and pos_info[1] == "普通名詞" and len(m.surface()) > 1:
                words.append({"word": m.surface(), "pos": ",".join(pos_info[0:4])})
    return words

# --- 各ワーカープロセスで実行される本体 ---
def worker_process_url(queue_item: dict, supabase_url: str, supabase_key: str, stop_words_set: set):
    """単一のHTML URLをダウンロード・解析・保存するワーカー関数"""
    global _WORKER_TOKENIZER
    if _WORKER_TOKENIZER is None:
        _WORKER_TOKENIZER = dictionary.Dictionary(dict_type="full").create(mode=tokenizer.Tokenizer.SplitMode.C)

    url = queue_item['url']
    print(f"[*] ワーカー (PID: {os.getpid()}) が処理開始: {url}")
    supabase: Client = create_client(supabase_url, supabase_key)

    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
        response.raise_for_status()

        # Content-TypeがHTMLでなければ処理を完了とする
        content_type = response.headers.get("content-type", "").lower()
        if "html" not in content_type:
            print(f"  [-] HTMLでないためスキップ: {content_type}")
            supabase.table("crawl_queue").update({
                "status": "completed", "processed_at": datetime.now(timezone.utc).isoformat()
            }).eq("id", queue_item['id']).execute()
            return True

        text = get_text_from_html(response.content)
        
        if text:
            new_words = analyze_with_sudachi(text, _WORKER_TOKENIZER)
            filtered_words = [w for w in new_words if w["word"] not in stop_words_set]
            if filtered_words:
                for word_data in filtered_words:
                    upsert_res = supabase.table("unique_words").upsert(
                        {"word": word_data["word"], "pos": word_data["pos"]},
                        on_conflict="word"
                    ).execute()
                    if upsert_res.data:
                        word_id = upsert_res.data[0]['id']
                        supabase.table("word_occurrences").insert({
                            "word_id": word_id, "source_url": url
                        }).execute()

        supabase.table("crawl_queue").update({
            "status": "completed", "processed_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", queue_item['id']).execute()
        return True

    except Exception as e:
        print(f"  [!] エラー発生: {url} - {e}")
        supabase.table("crawl_queue").update({
            "status": "failed", "processed_at": datetime.now(timezone.utc).isoformat(), "error_message": str(e)
        }).eq("id", queue_item['id']).execute()
        return False

# --- メイン処理 (キューの管理とワーカーへのディスパッチ) ---
def main():
    """キューからURLを取得し、プロセスプールに処理を依頼する"""
    supabase_url: str = os.environ.get("SUPABASE_URL")
    supabase_key: str = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: raise ValueError("環境変数を設定してください。")

    supabase_main: Client = create_client(supabase_url, supabase_key)
    print("--- コンテンツ解析処理開始 ---")

    response = supabase_main.table("stop_words").select("word").execute()
    stop_words_set = {item['word'] for item in response.data}
    print(f"[*] {len(stop_words_set)}件の除外ワードを読み込みました。")

    response = supabase_main.table("crawl_queue").select("id, url", count='exact').eq("status", "queued").limit(PROCESS_BATCH_SIZE).execute()
    urls_to_process = response.data
    if not urls_to_process:
        print("[*] 処理対象のURLがキューにありません。終了します。")
        return

    processing_ids = [item['id'] for item in urls_to_process]
    supabase_main.table("crawl_queue").update({"status": "processing"}).in_("id", processing_ids).execute()
    print(f"[*] {len(urls_to_process)}件のURLを処理対象としてロックしました。")

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(worker_process_url, item, supabase_url, supabase_key, stop_words_set) for item in urls_to_process]
        results = [f.result() for f in futures]
    
    success_count = sum(1 for r in results if r)
    print(f"\n--- コンテンツ解析処理終了 ---")
    print(f"今回処理したURL数: {len(results)} (成功: {success_count}, 失敗: {len(results) - success_count})")

if __name__ == "__main__":
    main()
