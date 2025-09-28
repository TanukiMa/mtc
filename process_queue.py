# process_queue.py (全文)
import re
import os
import time
import configparser
import requests
import hashlib
import tempfile
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor

from bs4 import BeautifulSoup
from supabase import create_client, Client
from sudachipy import tokenizer, dictionary
from sudachipy.user_dict import ubuild

_WORKER_TOKENIZER = None

def build_user_dict_from_table(supabase_client: Client, table_name: str) -> str:
    """Supabaseの指定されたテーブルからユーザー辞書をビルドする (JOIN使用)"""
    print(f"[*] Supabaseテーブル '{table_name}' からユーザー辞書データを取得しています...")
    try:
        # ▼▼▼▼▼ pos_masterテーブルをJOINして、品詞文字列を取得する ▼▼▼▼▼
        response = supabase_client.from_(table_name).select(
            "surface, sudachi_reading, reading, pos_master(pos1, pos2, pos3, pos4, pos5, pos6)"
        ).execute()
        # ▲▲▲▲▲ ここまで変更 ▲▲▲▲▲
        
        if not response.data:
            print(f"  [-] テーブル '{table_name}' にデータがありません。")
            return None
        
        print(f"  [+] {len(response.data)}件の単語データを取得しました。")
        
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv', encoding='utf-8') as f_csv:
            for item in response.data:
                # ▼▼▼▼▼ JOINした結果から品詞文字列を再構築する ▼▼▼▼▼
                pos_data = item.get('pos_master')
                if not pos_data: continue
                
                pos_parts = [
                    pos_data.get('pos1', '*'), pos_data.get('pos2', '*'),
                    pos_data.get('pos3', '*'), pos_data.get('pos4', '*'),
                    pos_data.get('pos5', '*'), pos_data.get('pos6', '*')
                ]
                pos_string = ",".join(p if p is not None else '*' for p in pos_parts)
                # ▲▲▲▲▲ ここまで変更 ▲▲▲▲▲

                line = f"{item['surface']},{item['sudachi_reading']},{item['reading']},{pos_string}\n"
                f_csv.write(line)
            csv_path = f_csv.name
        
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix=f'_{table_name}.dict') as f_dict:
            dict_path = f_dict.name

        print(f"[*] ユーザー辞書をビルドしています ({table_name})...")
        ubuild.build(csv_path, dict_path)
        print(f"  [+] ビルド完了: {dict_path}")
        
        os.remove(csv_path)
        return dict_path

    except Exception as e:
        print(f"  [!] テーブル '{table_name}' の辞書ビルド中にエラー: {e}")
        return None

# (get_content_hash, clean_text, get_text_from_html, analyze_with_sudachi, worker_process_url, main, if __name__ ... は一切変更ありません)
# ... (以下、前回のバージョンから変更なし) ...
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
                if m.is_oov() and pos_info[0] == "名詞" and pos_info[1] == "普通名詞" and len(m.surface()) > 1:
                    words.append({"word": m.surface(), "pos": ",".join(pos_info[0:4])})
    except Exception as e: print(f"  [!] Sudachi解析エラー: {e}")
    return words
def worker_process_url(queue_item: dict, supabase_url: str, supabase_key: str, stop_words_set: set, request_timeout: int, user_dict_paths: list):
    global _WORKER_TOKENIZER
    if _WORKER_TOKENIZER is None:
        _WORKER_TOKENIZER = dictionary.Dictionary(dict="full", user_dict=user_dict_paths).create(mode=tokenizer.Tokenizer.SplitMode.C)
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
                    upsert_res = supabase.table("unique_words").upsert({"word": word_data["word"], "pos": word_data["pos"]}, on_conflict="word").execute()
                    if upsert_res.data:
                        word_id = upsert_res.data[0]['id']
                        supabase.table("word_occurrences").insert({"word_id": word_id, "source_url": url}).execute()
        supabase.table("crawl_queue").update({"status": "completed", "content_hash": new_hash, "processed_at": datetime.now(timezone.utc).isoformat()}).eq("id", url_id).execute()
        return True
    except Exception as e:
        print(f"  [!] エラー発生: {url} - {e}")
        supabase.table("crawl_queue").update({"status": "failed", "processed_at": datetime.now(timezone.utc).isoformat(), "error_message": str(e)}).eq("id", url_id).execute()
        return False
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
    user_dict_paths = []
    dictionary_tables = ["general_user_dictionary", "medical_user_dictionary"]
    for table in dictionary_tables:
        path = build_user_dict_from_table(supabase_main, table)
        if path: user_dict_paths.append(path)
    print("--- コンテンツ解析処理開始 (一括処理・レートリミットモード) ---")
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
            futures = [executor.submit(worker_process_url, item, supabase_url, supabase_key, stop_words_set, request_timeout, user_dict_paths) for item in urls_to_process]
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
    for path in user_dict_paths:
        if path and os.path.exists(path):
            os.remove(path)
            print(f"[*] 一時ユーザー辞書ファイル {path} を削除しました。")
    print(f"\n--- コンテンツ解析処理終了 ---")
    print(f"今回処理した合計URL数: {total_processed_count}")
if __name__ == "__main__":
    main()
