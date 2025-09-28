# process_queue.py (全文)
import re
import os
import time
import configparser
import requests
import hashlib
import sys
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor

from bs4 import BeautifulSoup
from supabase import create_client, Client
from sudachipy import tokenizer, dictionary

_WORKER_TOKENIZER = None
_POS_CACHE = {}

def get_content_hash(content: bytes) -> str: return hashlib.sha256(content).hexdigest()
def clean_text(text: str) -> str:
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

def get_text_from_html(content: bytes) -> str:
    try:
        soup = BeautifulSoup(content, 'html.parser')
        for s in soup(['script', 'style']): s.decompose()
        return clean_text(' '.join(soup.stripped_strings))
    except Exception as e: raise RuntimeError(f"HTML解析エラー: {e}")

def analyze_with_sudachi(text: str, tokenizer_obj) -> list:
    if not text.strip() or not tokenizer_obj: return []
    chunk_size = 40000
    words = []
    try:
        for i in range(0, len(text), chunk_size):
            chunk = text[i:i + chunk_size]
            morphemes = tokenizer_obj.tokenize(chunk)
            for m in morphemes:
                pos_info = m.part_of_speech()
                pos_tuple = tuple(pos_info)
                pos_id = _POS_CACHE.get(pos_tuple)
                if m.is_oov() and pos_info[0] == "名詞" and pos_info[1] == "普通名詞" and len(m.surface()) > 1:
                    if pos_id is not None:
                        words.append({"word": m.surface(), "pos_id": pos_id})
    except Exception as e: print(f"  [!] Sudachi解析エラー: {e}", file=sys.stderr)
    return words

def worker_process_url(queue_item: dict, supabase_url: str, supabase_key: str, stop_words_set: set, request_timeout: int, user_dict_path: str, pos_cache: dict):
    global _WORKER_TOKENIZER, _POS_CACHE
    if _WORKER_TOKENIZER is None:
        # ▼▼▼▼▼ Tokenizerの初期化方法を修正 ▼▼▼▼▼
        dict_path = user_dict_path if user_dict_path and os.path.exists(user_dict_path) else None
        print(f"[*] ワーカー (PID: {os.getpid()}) でSudachi Tokenizerを初期化します (ユーザー辞書: {dict_path})。")
        _WORKER_TOKENIZER = dictionary.Dictionary(dict="full", user_dict=dict_path).create(mode=tokenizer.Tokenizer.SplitMode.C)
        # ▲▲▲▲▲ ここまで修正 ▲▲▲▲▲
        _POS_CACHE = pos_cache
    url_id, url = queue_item['id'], queue_item['url']
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
        text = get_text_from_html(response.content)
        if text:
            new_words = analyze_with_sudachi(text, _WORKER_TOKENIZER)
            filtered_words = [w for w in new_words if w["word"] not in stop_words_set]
            if filtered_words:
                supabase.table("word_occurrences").delete().eq("source_url", url).execute()
                for word_data in filtered_words:
                    upsert_res = supabase.table("unique_words").upsert({"word": word_data["word"], "pos_id": word_data["pos_id"]}, on_conflict="word").execute()
                    if upsert_res.data:
                        word_id = upsert_res.data[0]['id']
                        supabase.table("word_occurrences").insert({"word_id": word_id, "source_url": url}).execute()
        supabase.table("crawl_queue").update({"status": "completed", "content_hash": new_hash, "processed_at": datetime.now(timezone.utc).isoformat()}).eq("id", url_id).execute()
        return True
    except Exception as e:
        print(f"  [!] エラー発生: {url} - {e}", file=sys.stderr)
        supabase.table("crawl_queue").update({"status": "failed", "processed_at": datetime.now(timezone.utc).isoformat(), "error_message": str(e)}).eq("id", url_id).execute()
        return False

def load_pos_master_to_cache(supabase_client: Client) -> dict:
    pos_cache = {}
    print("[*] 品詞マスターデータをDBから読み込んでいます...")
    try:
        response = supabase_client.table("pos_master").select("id,pos1,pos2,pos3,pos4,pos5,pos6").execute()
        for item in response.data:
            key = tuple(item.get(f'pos{i}', '*') for i in range(1, 7))
            pos_cache[key] = item['id']
        print(f"  [+] {len(pos_cache)}件の品詞データをキャッシュしました。")
    except Exception as e: print(f"  [!] 品詞マスターの読み込みに失敗: {e}", file=sys.stderr)
    return pos_cache

def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    max_workers = config.getint('Processor', 'MAX_WORKERS')
    process_batch_size = config.getint('Processor', 'PROCESS_BATCH_SIZE')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    target_accesses_per_minute = config.getint('RateLimit', 'TARGET_ACCESSES_PER_MINUTE')
    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: raise ValueError("環境変数を設定してください。")
    
    # ▼▼▼▼▼ 正しい環境変数名でパスを取得 ▼▼▼▼▼
    user_dict_path = os.environ.get("USER_DICT_PATH")
    # ▲▲▲▲▲ ここまで修正 ▲▲▲▲▲
    
    supabase_main = create_client(supabase_url, supabase_key)
    print("--- コンテンツ解析処理開始 ---")
    pos_cache_for_workers = load_pos_master_to_cache(supabase_main)
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
        print(f"[*] {len(urls_to_process)}件のURLをロックしました。")
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # ワーカーに user_dict_path を渡す
            futures = [executor.submit(worker_process_url, item, supabase_url, supabase_key, stop_words_set, request_timeout, user_dict_path, pos_cache_for_workers) for item in urls_to_process]
            results = [f.result() for f in futures]
        success_count = sum(1 for r in results if r)
        batch_count = len(results)
        total_processed_count += batch_count
        print(f"  [+] 1バッチ処理完了 (成功: {success_count}, 失敗: {batch_count - success_count})")
        elapsed_time = time.time() - batch_start_time
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
