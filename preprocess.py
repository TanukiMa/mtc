import os
import sys
import re
import time
import configparser
import requests
import hashlib
import csv
import argparse  # --- 修正点: argparseをインポート ---
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, TimeoutError
from bs4 import BeautifulSoup
import chardet
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sudachipy import tokenizer, dictionary

# SQLAlchemy関連のインポート
from db_utils import get_local_db_session, CrawlQueue, SentenceQueue, BoilerplatePattern

# (init_worker, extract_and_split_sentences は変更ありません)
_WORKER_TOKENIZER = None
_WORK_BOILERPLATE_PATTERNS = []
def init_worker(dict_type: str):
    global _WORKER_TOKENIZER, _WORK_BOILERPLATE_PATTERNS
    if _WORKER_TOKENIZER is None:
        _WORKER_TOKENIZER = dictionary.Dictionary(dict=dict_type).create()
    if not _WORK_BOILERPLATE_PATTERNS:
        session = get_local_db_session()
        try:
            patterns = session.query(BoilerplatePattern).all()
            _WORK_BOILERPLATE_PATTERNS = [p.pattern for p in patterns]
        finally:
            session.close()

def extract_and_split_sentences(content: bytes, min_len: int) -> list:
    SAFE_CHUNK_BYTES = 40000
    try:
        soup = BeautifulSoup(content, 'html5lib')
        for s in soup(["script", "style", "header", "footer", "nav", "aside", "form"]):
            s.decompose()
        all_text = soup.get_text(separator="\n", strip=True)
        if not _WORKER_TOKENIZER: raise RuntimeError("Tokenizer is not initialized.")
        all_sentences_from_text = []
        text_bytes = all_text.encode('utf-8')
        start = 0
        while start < len(text_bytes):
            end = start + SAFE_CHUNK_BYTES
            while end < len(text_bytes) and (text_bytes[end] & 0xC0) == 0x80: end -= 1
            chunk_str = text_bytes[start:end].decode('utf-8', 'ignore')
            sentences_in_chunk = [str(s) for s in _WORKER_TOKENIZER.tokenize(chunk_str, mode=tokenizer.Tokenizer.SplitMode.A)]
            all_sentences_from_text.extend(sentences_in_chunk)
            start = end
        clean_sentences = []
        for s in all_sentences_from_text:
            s = re.sub(r'\s+', ' ', s).strip()
            if len(s) >= min_len and not any(pat in s for pat in _WORK_BOILERPLATE_PATTERNS):
                clean_sentences.append(s)
        return clean_sentences
    except Exception as e:
        print(f"   [!] HTML parsing or sentence splitting error: {e}", file=sys.stderr)
        return []

# --- Main worker function ---
# --- 修正点: is_debug_mode 引数を追加 ---
def worker_preprocess_url(queue_item_id: int, request_timeout: int, min_sentence_length: int, is_debug_mode: bool) -> tuple:
    session = get_local_db_session()
    url_to_process = ""
    try:
        queue_item = session.query(CrawlQueue).filter(
            CrawlQueue.id == queue_item_id,
            CrawlQueue.extraction_status == 'queued'
        ).with_for_update(skip_locked=True).one_or_none()

        if not queue_item:
            # --- 修正点: is_debug_modeに応じて戻り値の形式を変える ---
            return (queue_item_id, "skipped", url_to_process, []) if is_debug_mode else (queue_item_id, "skipped")
        
        url_to_process = queue_item.url
        queue_item.extraction_status = 'processing'
        session.commit()

        # ... (HTTPリクエストとHTML解析) ...
        http_session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        http_session.mount('https://', HTTPAdapter(max_retries=retries))
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = http_session.get(url_to_process, timeout=request_timeout, headers=headers, allow_redirects=True)
        response.raise_for_status()
        content_type = response.headers.get("content-type", "").lower()

        if "html" not in content_type:
            queue_item.extraction_status = "completed"
            session.commit()
            return (queue_item_id, "completed_non_html", url_to_process, []) if is_debug_mode else (queue_item_id, "completed_non_html")

        new_hash = hashlib.sha256(response.content).hexdigest()
        if queue_item.content_hash and queue_item.content_hash == new_hash:
            queue_item.extraction_status = "completed"
            session.commit()
            return (queue_item_id, "completed_not_modified", url_to_process, []) if is_debug_mode else (queue_item_id, "completed_not_modified")

        sentences = extract_and_split_sentences(response.content, min_sentence_length)
        
        session.query(SentenceQueue).filter_by(crawl_queue_id=queue_item.id).delete(synchronize_session=False)
        
        if sentences:
            session.bulk_insert_mappings(SentenceQueue, [{"crawl_queue_id": queue_item.id, "sentence_text": s} for s in sentences])

        queue_item.extraction_status = "completed"
        queue_item.content_hash = new_hash
        queue_item.processed_at = datetime.now(timezone.utc)
        session.commit()
        return (queue_item_id, "completed_success", url_to_process, sentences) if is_debug_mode else (queue_item_id, "completed_success")

    except Exception as e:
        if session.is_active: session.rollback()
        error_session = get_local_db_session()
        try:
            error_session.query(CrawlQueue).filter_by(id=queue_item_id).update({"extraction_status": "failed"})
            error_session.commit()
        finally:
            error_session.close()
        return (queue_item_id, f"failed: {e}", url_to_process, []) if is_debug_mode else (queue_item_id, f"failed: {e}")
    finally:
        if session.is_active:
            session.close()

# --- Main process orchestrator ---
def main():
    # --- 修正点: コマンドライン引数を解析 ---
    parser = argparse.ArgumentParser(description="Preprocess URLs from the crawl queue.")
    parser.add_argument('--debug', action='store_true', help="Enable debug mode to output a CSV artifact.")
    args = parser.parse_args()

    if args.debug:
        print("[*] Debug mode enabled. A CSV artifact will be generated.")

    config = configparser.ConfigParser()
    config.read('config.ini')
    max_workers = config.getint('Preprocessor', 'MAX_WORKERS')
    batch_size = config.getint('Preprocessor', 'BATCH_SIZE')
    req_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    sudachi_dict_type = config.get('Preprocessor', 'SUDACHI_DICT_TYPE', fallback='full')
    min_sentence_length = config.getint('Preprocessor', 'MIN_SENTENCE_LENGTH', fallback=10)
    
    session = get_local_db_session()
    print("--- Text Extraction Process Started ---")
    
    # --- 修正点: デバッグモードの時だけリストを初期化 ---
    debug_results_for_csv = [] if args.debug else None

    try:
        while True:
            items_to_process = session.query(CrawlQueue.id).filter(
                CrawlQueue.extraction_status == 'queued'
            ).order_by(CrawlQueue.id).limit(batch_size).all()
            
            if not items_to_process:
                print("[*] No URLs to preprocess in queue. Exiting.")
                break
            
            ids_to_process = [item.id for item in items_to_process]
            print(f"[*] Processing batch of {len(ids_to_process)} URLs...")

            with ProcessPoolExecutor(max_workers=max_workers, initializer=init_worker, initargs=(sudachi_dict_type,)) as executor:
                # --- 修正点: is_debug_modeフラグをワーカーに渡す ---
                futures = [executor.submit(worker_preprocess_url, item_id, req_timeout, min_sentence_length, args.debug) for item_id in ids_to_process]
                
                for future in futures:
                    try:
                        result = future.result(timeout=req_timeout + 60)
                        
                        # --- 修正点: デバッグモードの場合のみ結果を詳細に処理 ---
                        if args.debug:
                            item_id, status, url, extracted_sentences = result
                            if "failed" in status:
                                print(f"   [!] Worker for ID {item_id} failed. Reason: {status}")
                            if extracted_sentences:
                                for sentence in extracted_sentences:
                                    debug_results_for_csv.append({"url": url, "sentence": sentence})
                        else:
                            item_id, status = result
                            if "failed" in status:
                                print(f"   [!] Worker for ID {item_id} failed. Reason: {status}")

                    except TimeoutError:
                        print(f"   [!] A worker process timed out.", file=sys.stderr)
                    except Exception as e:
                        print(f"   [!] A future raised an exception: {e}", file=sys.stderr)
            
            print(f"   [+] Batch complete.")
    finally:
        session.close()

    # --- 修正点: デバッグモードで、かつ結果がある場合のみCSVを書き出す ---
    if args.debug and debug_results_for_csv:
        print("\n[*] Writing debug artifact...")
        try:
            with open('preprocess_debug_output.csv', 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=["url", "sentence"])
                writer.writeheader()
                writer.writerows(debug_results_for_csv)
            print("[+] Debug artifact written to 'preprocess_debug_output.csv'")
        except Exception as e:
            print(f"[!] Failed to write debug artifact: {e}", file=sys.stderr)

    print("--- Text Extraction Process Finished ---")

if __name__ == "__main__":
    main()
