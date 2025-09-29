# process_ginza.py
import os, sys, re, time, configparser, requests, hashlib
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor
from bs4 import BeautifulSoup
from supabase import create_client, Client
import spacy

# --- Globals for worker processes ---
_NLP_MODEL = None

# --- Helper Functions ---
def get_content_hash(content: bytes) -> str: return hashlib.sha256(content).hexdigest()
def clean_text(text: str) -> str:
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()
def get_text_from_html(content: bytes) -> str:
    try:
        soup = BeautifulSoup(content, 'html.parser')
        for s in soup(['script', 'style']): s.decompose()
        return clean_text(' '.join(soup.stripped_strings))
    except Exception as e: raise RuntimeError(f"HTML parsing error: {e}")

def analyze_with_ginza(text: str) -> list:
    global _NLP_MODEL
    if not text.strip() or _NLP_MODEL is None: return []
    
    # Sudachiのバイト数制限エラーを回避
    chunk_size = 40000
    found_words = []
    # 抽出済み単語を記録し、重複を防ぐ
    found_texts = set()

    try:
        for i in range(0, len(text), chunk_size):
            chunk = text[i:i + chunk_size]
            doc = _NLP_MODEL(chunk)
            
            # 1. 固有表現(人名、組織名、製品名など)をすべて抽出
            for ent in doc.ents:
                word_text = ent.text.strip()
                if len(word_text) > 1 and word_text not in found_texts:
                    found_words.append({"word": word_text, "source_tool": "ginza", "entity_category": ent.label_, "pos_tag": "ENT"})
                    found_texts.add(word_text)

            # 2. 上記以外で、品詞が「名詞」のものを抽出
            for token in doc:
                word_text = token.text.strip()
                if token.pos_ == "NOUN" and len(word_text) > 1 and word_text not in found_texts:
                     found_words.append({"word": word_text, "source_tool": "ginza", "entity_category": "NOUN_GENERAL", "pos_tag": token.tag_})
                     found_texts.add(word_text)

    except Exception as e:
        print(f"  [!] GiNZA analysis error: {e}", file=sys.stderr)
    return found_words

def worker_process_url(queue_item, supabase_url, supabase_key, request_timeout, request_delay):
    global _NLP_MODEL
    if _NLP_MODEL is None:
        print(f"[*] Worker (PID: {os.getpid()}) loading GiNZA model 'ja_ginza_electra' from cache...")
        # 事前ダウンロードしたモデルをキャッシュから読み込む
        _NLP_MODEL = spacy.load("ja_ginza_electra")

    url_id, url = queue_item['id'], queue_item['url']
    supabase: Client = create_client(supabase_url, supabase_key)

    try:
        time.sleep(request_delay)
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, timeout=request_timeout, headers=headers)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "").lower()
        if "html" not in content_type:
            supabase.table("crawl_queue").update({"status": "completed"}).eq("id", url_id).execute()
            return True
        
        new_hash = get_content_hash(response.content)
        
        text = get_text_from_html(response.content)
        if text:
            new_words = analyze_with_ginza(text)
            if new_words:
                supabase.table("word_occurrences").delete().eq("source_url", url).execute()
                # stop_wordsでのフィルタリングは省略 (必要なら追加)
                for word_data in new_words:
                    upsert_res = supabase.table("unique_words").upsert(word_data, on_conflict="word, source_tool").execute()
                    if upsert_res.data:
                        word_id = upsert_res.data[0]['id']
                        supabase.table("word_occurrences").insert({"word_id": word_id, "source_url": url}).execute()

        supabase.table("crawl_queue").update({"status": "completed", "content_hash": new_hash, "processed_at": datetime.now(timezone.utc).isoformat()}).eq("id", url_id).execute()
        return True
    except Exception as e:
        print(f"  [!] エラー発生: {url} - {e}", file=sys.stderr)
        supabase.table("crawl_queue").update({"status": "failed", "error_message": str(e)}).eq("id", url_id).execute()
        return False

def main():
    config = configparser.ConfigParser(); config.read('config.ini')
    max_workers = config.getint('Processor', 'MAX_WORKERS')
    batch_size = config.getint('Processor', 'PROCESS_BATCH_SIZE')
    req_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    req_delay = config.getfloat('RateLimit', 'REQUEST_DELAY_SECONDS')
    
    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    supabase = create_client(supabase_url, supabase_key)
    print("--- GiNZA Content Processor Started ---")

    while True:
        res = supabase.table("crawl_queue").select("id, url").eq("status", "queued").limit(batch_size).execute()
        if not res.data: 
            print("[*] 処理対象のURLがキューにありません。終了します。")
            break
        
        ids = [item['id'] for item in res.data]
        supabase.table("crawl_queue").update({"status": "processing"}).in_("id", ids).execute()
        print(f"[*] {len(res.data)}件のURLをロックしました。")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(worker_process_url, item, supabase_url, supabase_key, req_timeout, req_delay) for item in res.data]
            results = [f.result() for f in futures]
            success_count = sum(1 for r in results if r)
            print(f"  [+] 1バッチ処理完了 (成功: {success_count}, 失敗: {len(results) - success_count})")
            
    print("--- GiNZA Content Processor Finished ---")

if __name__ == "__main__":
    main()
