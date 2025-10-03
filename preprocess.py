# preprocess.py
import os, sys, re, time, configparser, requests, hashlib
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor
from bs4 import BeautifulSoup
import chardet
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sudachipy import tokenizer, dictionary

# SQLAlchemy関連のインポート
from sqlalchemy.orm import joinedload
from db_utils import get_local_db_session, CrawlQueue, SentenceQueue

# --- Globals for worker processes ---
_WORKER_TOKENIZER = None

# --- Helper Functions ---
def get_sentences_from_html(content: bytes, safe_byte_limit: int, char_chunk_size: int) -> list:
    final_sentences = []
    try:
        soup = BeautifulSoup(content, 'html5lib')
        for s in soup(["script", "style", "header", "footer", "nav", "aside", "form"]):
            s.decompose()
        
        for tag in soup.find_all(['div', 'ul', 'ol', 'table', 'tbody', 'tr']):
            tag.unwrap()
        
        content_blocks = soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'th', 'td'])
        
        all_sentences = []
        for block in content_blocks:
            block_text = re.sub(r'\s+', ' ', block.get_text(strip=True))
            if not block_text:
                continue
            sentences_in_block = re.split(r'(?<=[。！？])\s*', block_text)
            all_sentences.extend(sentences_in_block)

        clean_sentences = []
        for s in all_sentences:
            s = s.strip()
            if len(s) > 10 and "ページの先頭へ戻る" not in s:
                clean_sentences.append(s)
        
        for sentence in clean_sentences:
            if len(sentence.encode('utf-8')) > safe_byte_limit:
                for i in range(0, len(sentence), char_chunk_size):
                    final_sentences.append(sentence[i:i + char_chunk_size])
            else:
                final_sentences.append(sentence)
        
        return final_sentences
    except Exception as e:
        raise RuntimeError(f"HTML parsing error: {e}")

def filter_sentences_with_oov(sentences: list) -> list:
    global _WORKER_TOKENIZER
    if _WORKER_TOKENIZER is None:
        _WORKER_TOKENIZER = dictionary.Dictionary(dict="full").create(mode=tokenizer.Tokenizer.SplitMode.C)
    
    interesting_sentences = []
    for sentence in sentences:
        try:
            if any(m.is_oov() for m in _WORKER_TOKENIZER.tokenize(sentence)):
                interesting_sentences.append(sentence)
        except Exception as e:
            print(f"  [!] Sudachi pre-filtering error: {e}", file=sys.stderr)
    return interesting_sentences

# --- Main worker function ---
def worker_preprocess_url(queue_item_id, request_timeout, safe_byte_limit, char_chunk_size):
    session = get_local_db_session()
    try:
        queue_item = session.query(CrawlQueue).filter_by(id=queue_item_id).one_or_none()
        if not queue_item:
            return

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
            return

        if response.encoding == 'ISO-8859-1':
            encoding = chardet.detect(response.content)['encoding']
            if encoding:
                response.encoding = encoding
                
        new_hash = hashlib.sha256(response.content).hexdigest()

        if queue_item.content_hash and queue_item.content_hash == new_hash:
            queue_item.extraction_status = "completed"
            session.commit()
            return

        sentences = get_sentences_from_html(response.content, safe_byte_limit, char_chunk_size)
        
        session.query(SentenceQueue).filter_by(crawl_queue_id=queue_item.id).delete(synchronize_session=False)
        
        if sentences:
            interesting_sentences = filter_sentences_with_oov(sentences)
            if interesting_sentences:
                session.bulk_insert_mappings(SentenceQueue, [{"crawl_queue_id": queue_item.id, "sentence_text": s} for s in interesting_sentences])

        queue_item.extraction_status = "completed"
        queue_item.content_hash = new_hash
        queue_item.last_modified = response.headers.get('Last-Modified')
        queue_item.etag = response.headers.get('ETag')
        queue_item.processed_at = datetime.now(timezone.utc)
        session.commit()

    except Exception as e:
        print(f"  [!] Preprocessing error on URL ID {queue_item_id}: {e}", file=sys.stderr)
        session.rollback()
        session.query(CrawlQueue).filter_by(id=queue_item_id).update({"extraction_status": "failed"})
        session.commit()
    finally:
        session.close()

# --- Main process orchestrator ---
def main():
    config = configparser.ConfigParser(); config.read('config.ini')
    max_workers = config.getint('Preprocessor', 'MAX_WORKERS')
    batch_size = config.getint('Preprocessor', 'BATCH_SIZE')
    req_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    safe_byte_limit = config.getint('Preprocessor', 'SAFE_BYTE_LIMIT')
    char_chunk_size = config.getint('Preprocessor', 'CHAR_CHUNK_SIZE')
    
    session = get_local_db_session()
    print("--- Text Extraction Process Started (Local DB Mode) ---")

    while True:
        items_to_process = session.query(CrawlQueue.id).filter_by(extraction_status='queued').limit(batch_size).all()
        if not items_to_process:
            print("[*] No URLs to preprocess in queue. Exiting.")
            break
        
        ids_to_process = [item.id for item in items_to_process]
        
        session.query(CrawlQueue).filter(CrawlQueue.id.in_(ids_to_process)).update({"extraction_status": "processing"}, synchronize_session=False)
        session.commit()
        print(f"[*] Locked {len(ids_to_process)} URLs for extraction.")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(worker_preprocess_url, item_id, req_timeout, safe_byte_limit, char_chunk_size) for item_id in ids_to_process]
            results = [f.result() for f in futures]
            print(f"  [+] Batch of {len(futures)} tasks complete.")
    
    session.close()
    print("--- Text Extraction Process Finished ---")

if __name__ == "__main__":
    main()