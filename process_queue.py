# process_queue.py
import re
import os
import time
import configparser
import requests
import hashlib
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor

from bs4 import BeautifulSoup
from supabase import create_client, Client
from sudachipy import tokenizer, dictionary

# --- Sudachi Tokenizerのためのグローバル変数 ---
_WORKER_TOKENIZER = None

# --- テキスト抽出・解析関連の関数群 (内容は変更なし) ---
def get_content_hash(content: bytes) -> str:
    return hashlib.sha2sha256(content).hexdigest()

def clean_text(text: str) -> str:
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

def get_text_from_html(content: bytes) -> str:
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
def worker_process_url(queue_item: dict, supabase_url: str, supabase_key: str, stop_words_set: set, request_timeout: int):
    global _WORKER_TOKENIZER
    if _WORKER_TOKENIZER is None:
        _WORKER_TOKENIZER = dictionary.Dictionary(dict="full").create(mode=tokenizer.Tokenizer.SplitMode.C)

    url_id, url = queue_item['id'], queue_item['url']
    # print(f"[*] ワーカー (PID: {os.getpid()}) が処理開始: {url}")
    supabase: Client = create_client(supabase_url, supabase_key)

    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, timeout=request_timeout, headers=headers)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "").lower()
        if "html" not in content_type:
            supabase.table("crawl_queue").update({"status": "completed", "processed_at": datetime.now(timezone.utc).isoformat()}).eq("id", url_id).execute()
            return True

        new_hash = get_content_hash(response.content)
        db_res = supabase.table("crawl_queue").select("content_hash").eq("id", url_id).single().execute()
        old_hash = db_res.data.get("content_hash") if db_res.data else None
        
        if old_hash == new_hash:
            supabase.table("crawl_queue").update({"status": "completed", "processed_at": datetime.now(timezone.utc).isoformat()}).eq("id", url_id).execute()
            return True
        
        # print(f"  [+] 新規または更新されたHTMLコンテンツを解析します。")
        text = get_text_from_html(response.content)
        
        if text:
            new_words = analyze_with_sudachi(text, _WORKER_TOKENIZER)
            filtered_words = [w for w in new_words if w["word"] not in stop_words_set]
            
            if filtered_words:
                # ▼▼▼▼▼ この1行を追加 ▼▼▼▼▼
                # 新しい単語を登録する前に、このURLに関する古い出現記録をすべて削除
                supabase.table("word_occurrences").delete().eq("source_url", url).execute()
                # ▲▲▲▲▲ ▲▲▲▲▲ ▲▲▲▲▲

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
            "status": "completed", "content_hash": new_hash, "processed_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", url_id).execute()
        return True

    except Exception as e:
        print(f"  [!] エラー発生: {url} - {e}")
        supabase.table("crawl_queue").update({
            "status": "failed", "processed_at": datetime.now(timezone.utc).isoformat(), "error_message": str(e)
        }).eq("id", url_id).execute()
        return False

# --- メイン処理 (内容は変更なし) ---
def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    max_workers = config.getint('Processor', 'MAX_WORKERS')
    process_batch_size = config.getint('Processor', 'PROCESS_BATCH_SIZE')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    target_accesses_per_minute = config.getint('RateLimit', 'TARGET_ACCESSES_PER_MINUTE')

    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: raise ValueError("環境変数を設定してください。")

    supabase_main = create_client(supabase_url, supabase_key)
    print("--- コンテンツ解析処理開始 (レートリミットモード) ---")

    response = supabase_main.table("stop_words").select("word").execute()
    stop_words_set = {item['word'] for item in response.data}
    print(f"[*] {len(stop_words_set)}件の除外ワードを読み込みました。")

    total_processed_count = 0
    
    while True:
        batch_start_time = time.time()
        response = supabase_main.table("crawl_queue").select("id, url").in_("status", ["queued", "failed"]).limit(process_batch_size).execute()
        urls_to_process = response.data
        if not urls_to_process:
            print("[*] 処理対象のURLがキューにありません。終了します。")
            break
        processing_ids = [item['id'] for item in urls_to_process]
        supabase_main.table("crawl_queue").update({"status": "processing"}).in_("id", processing_ids).execute()
        print(f"[*] {len(urls_to_process)}件のURLを処理対象としてロックしました。")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(worker_process_url, item, supabase_url, supabase_key, stop_words_set, request_timeout) for item in urls_to_process]
            results = [f.result() for f in futures]
        
        success_count = sum(1 for r in results if r)
        batch_count = len(results)
        total_processed_count += batch_count
        print(f"  [+] 1バッチ処理完了 (成功: {success_count}, 失敗: {batch_count - success_count})")
        
        batch_end_time = time.time()
        elapsed_time = batch_end_time - batch_start_time
        if target_accesses_per_minute > 0:
            required_time = (60.0 / target_accesses_per_minute) * batch_count
            if elapsed_time < required_time:
                wait_time = required_time - elapsed_time
                print(f"  [*] レートリミットのため {wait_time:.2f} 秒待機します。")
                time.sleep(wait_time)
    
    print(f"\n--- コンテンツ解析処理終了 ---")
    print(f"今回処理した合計URL数: {total_processed_count}")

if __name__ == "__main__":
    main()
