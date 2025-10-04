import os, sys, re, time, configparser, requests, hashlib
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, TimeoutError
from bs4 import BeautifulSoup
import chardet
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sudachipy import tokenizer, dictionary

# SQLAlchemy関連のインポート
from db_utils import get_local_db_session, CrawlQueue, SentenceQueue, BoilerplatePattern

# --- Globals for worker processes ---
_WORKER_TOKENIZER = None
_WORK_BOILERPLATE_PATTERNS = []

# --- Worker Initializer (各ワーカープロセス起動時に一度だけ実行) ---
def init_worker(dict_type: str):
    """
    ワーカープロセスのグローバル変数を初期化する
    """
    global _WORKER_TOKENIZER, _WORK_BOILERPLATE_PATTERNS
    
    # 1. SudachiPy Tokenizerの初期化
    if _WORKER_TOKENIZER is None:
        _WORKER_TOKENIZER = dictionary.Dictionary(dict=dict_type).create()

    # 2. 除外パターンの読み込み
    if not _WORK_BOILERPLATE_PATTERNS:
        session = get_local_db_session()
        try:
            patterns = session.query(BoilerplatePattern).all()
            _WORK_BOILERPLATE_PATTERNS = [p.pattern for p in patterns]
        finally:
            session.close()

# --- Helper Functions ---
def extract_and_split_sentences(content: bytes, min_len: int) -> list:
    final_sentences = []
    try:
        soup = BeautifulSoup(content, 'html5lib')
        for s in soup(["script", "style", "header", "footer", "nav", "aside", "form"]):
            s.decompose()
        
        all_text = soup.get_text(separator="\n", strip=True)
        
        if not _WORKER_TOKENIZER:
             raise RuntimeError("Tokenizer is not initialized in worker.")
        
        sentences = [str(s) for s in _WORKER_TOKENIZER.tokenize(all_text, mode=tokenizer.Tokenizer.SplitMode.A)]

        clean_sentences = []
        for s in sentences:
            s = re.sub(r'\s+', ' ', s).strip()
            if len(s) >= min_len and not any(pat in s for pat in _WORK_BOILERPLATE_PATTERNS):
                clean_sentences.append(s)
        
        return clean_sentences
    except Exception as e:
        # HTML解析エラーは致命的ではないため、空リストを返して処理を続行
        print(f"   [!] HTML parsing error: {e}", file=sys.stderr)
        return []

# --- Main worker function ---
def worker_preprocess_url(queue_item_id: int, request_timeout: int, min_sentence_length: int) -> tuple:
    """
    単一のURLを処理するワーカー関数。完全に独立したDBセッションを持つ。
    """
    session = get_local_db_session()
    try:
        # --- トランザクション開始: まず対象行をロックして取得 ---
        queue_item = session.query(CrawlQueue).filter(
            CrawlQueue.id == queue_item_id,
            CrawlQueue.extraction_status == 'queued'
        ).with_for_update(skip_locked=True).one_or_none()

        # 他のプロセスにロックされたか、既に対象でなくなっていた場合は何もせず終了
        if not queue_item:
            return queue_item_id, "skipped", 0
        
        # 処理中にステータスを変更
        queue_item.extraction_status = 'processing'
        session.commit()

        # --- HTTPリクエストとHTML解析 ---
        url = queue_item.url
        http_session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        http_session.mount('https://', HTTPAdapter(max_retries=retries))

        headers = {'User-Agent': 'Mozilla/5.0'}
        response = http_session.get(url, timeout=request_timeout, headers=headers, allow_redirects=True)
        response.raise_for_status()
        
        content_type = response.headers.get("content-type", "").lower()
        if "html" not in content_type:
            queue_item.extraction_status = "completed"
            session.commit()
            return queue_item_id, "completed_non_html", 0

        new_hash = hashlib.sha256(response.content).hexdigest()
        if queue_item.content_hash and queue_item.content_hash == new_hash:
            queue_item.extraction_status = "completed"
            session.commit()
            return queue_item_id, "completed_not_modified", 0

        # --- テキスト抽出とDB保存 ---
        sentences = extract_and_split_sentences(response.content, min_sentence_length)
        
        # 既存の文章を一度削除
        session.query(SentenceQueue).filter_by(crawl_queue_id=queue_item.id).delete(synchronize_session=False)
        
        if sentences:
            session.bulk_insert_mappings(SentenceQueue, [{"crawl_queue_id": queue_item.id, "sentence_text": s} for s in sentences])

        queue_item.extraction_status = "completed"
        queue_item.content_hash = new_hash
        queue_item.processed_at = datetime.now(timezone.utc)
        session.commit()
        return queue_item_id, "completed_success", len(sentences)

    except Exception as e:
        # エラー発生時はロールバックし、ステータスを'failed'に更新
        if session.is_active:
            session.rollback()
        
        # 新しいセッションで失敗ステータスを記録
        error_session = get_local_db_session()
        try:
            error_session.query(CrawlQueue).filter_by(id=queue_item_id).update({"extraction_status": "failed"})
            error_session.commit()
        finally:
            error_session.close()
            
        return queue_item_id, f"failed: {e}", 0
    finally:
        if session.is_active:
            session.close()

# --- Main process orchestrator ---
def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    max_workers = config.getint('Preprocessor', 'MAX_WORKERS')
    batch_size = config.getint('Preprocessor', 'BATCH_SIZE')
    req_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    sudachi_dict_type = config.get('Preprocessor', 'SUDACHI_DICT_TYPE', fallback='full')
    min_sentence_length = config.getint('Preprocessor', 'MIN_SENTENCE_LENGTH', fallback=10)
    
    session = get_local_db_session()
    print("--- Text Extraction Process Started ---")

    try:
        while True:
            # メインプロセスは処理対象のIDリストを取得するだけ
            items_to_process = session.query(CrawlQueue.id).filter(
                CrawlQueue.extraction_status == 'queued'
            ).order_by(CrawlQueue.id).limit(batch_size).all()
            
            if not items_to_process:
                print("[*] No URLs to preprocess in queue. Exiting.")
                break
            
            ids_to_process = [item.id for item in items_to_process]
            print(f"[*] Processing batch of {len(ids_to_process)} URLs...")

            with ProcessPoolExecutor(max_workers=max_workers, initializer=init_worker, initargs=(sudachi_dict_type,)) as executor:
                futures = [executor.submit(worker_preprocess_url, item_id, req_timeout, min_sentence_length) for item_id in ids_to_process]
                
                for future in futures:
                    try:
                        # ワーカーからの結果を受け取る（必要に応じてログ出力など）
                        item_id, status, count = future.result(timeout=req_timeout + 60)
                        if "failed" in status:
                             print(f"   [!] Worker for ID {item_id} failed. Reason: {status}")
                    except TimeoutError:
                        print(f"   [!] A worker process timed out.", file=sys.stderr)
                    except Exception as e:
                        print(f"   [!] A future raised an exception: {e}", file=sys.stderr)
            
            print(f"   [+] Batch complete.")
    finally:
        session.close()
    
    print("--- Text Extraction Process Finished ---")

if __name__ == "__main__":
    main()