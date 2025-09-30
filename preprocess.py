# preprocess.py
import os
import sys
import re
import time
import configparser
import requests
import hashlib
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor
from bs4 import BeautifulSoup
from supabase import create_client, Client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sudachipy import tokenizer, dictionary

# --- Globals for worker processes ---
_WORKER_TOKENIZER = None

# --- Helper Functions ---
def get_sentences_from_html(content: bytes, safe_byte_limit: int, char_chunk_size: int) -> list:
    """
    Extracts meaningful text blocks from HTML content, ensuring they do not exceed a byte limit.
    """
    final_sentences = []
    try:
        soup = BeautifulSoup(content, 'html.parser')
        for s in soup(["script", "style", "header", "footer", "nav", "aside"]):
            s.decompose()
        
        target_tags = ['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'div']
        raw_blocks = [re.sub(r'\s+', ' ', element.get_text(strip=True)) for element in soup.find_all(target_tags)]

        for block in filter(None, raw_blocks):
            if len(block.encode('utf-8')) > safe_byte_limit:
                for i in range(0, len(block), char_chunk_size):
                    final_sentences.append(block[i:i + char_chunk_size])
            else:
                final_sentences.append(block)
        
        return final_sentences
    except Exception as e:
        raise RuntimeError(f"HTML parsing error: {e}")

def filter_sentences_with_oov(sentences: list) -> list:
    """
    Filters a list of sentences, returning only those that contain at least one Out-of-Vocabulary (OOV) word.
    """
    global _WORKER_TOKENIZER
    if _WORKER_TOKENIZER is None:
        _WORKER_TOKENIZER = dictionary.Dictionary(dict="full").create(mode=tokenizer.Tokenizer.SplitMode.C)
    
    interesting_sentences = []
    for sentence in sentences:
        try:
            # `any()` provides an efficient short-circuiting check.
            if any(m.is_oov() for m in _WORKER_TOKENIZER.tokenize(sentence)):
                interesting_sentences.append(sentence)
        except Exception as e:
            print(f"  [!] Sudachi pre-filtering error: {e}", file=sys.stderr)
    return interesting_sentences

# --- Main worker function executed in each process ---
def worker_preprocess_url(queue_item, supabase_url, supabase_key, request_timeout, safe_byte_limit, char_chunk_size):
    """
    Downloads a URL, checks for changes, and if new/updated, extracts and pre-filters sentences.
    """
    url_id, url = queue_item['id'], queue_item['url']
    old_hash = queue_item.get('content_hash')
    old_last_modified = queue_item.get('last_modified')
    old_etag = queue_item.get('etag')

    supabase = create_client(supabase_url, supabase_key)
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        head_response = session.head(url, timeout=request_timeout, headers=headers, allow_redirects=True)
        head_response.raise_for_status()
        new_last_modified = head_response.headers.get('Last-Modified')
        new_etag = head_response.headers.get('ETag')

        if (old_last_modified and old_last_modified == new_last_modified) or \
           (old_etag and old_etag == new_etag):
            supabase.table("crawl_queue").update({"extraction_status": "completed", "processed_at": datetime.now(timezone.utc).isoformat()}).eq("id", url_id).execute()
            return True

        response = session.get(url, timeout=request_timeout, headers=headers, allow_redirects=True)
        response.raise_for_status()
        new_hash = hashlib.sha256(response.content).hexdigest()

        if old_hash and old_hash == new_hash:
            supabase.table("crawl_queue").update({"extraction_status": "completed", "last_modified": new_last_modified, "etag": new_etag, "processed_at": datetime.now(timezone.utc).isoformat()}).eq("id", url_id).execute()
            return True

        # Extract all sentences first.
        sentences = get_sentences_from_html(response.content, safe_byte_limit, char_chunk_size)
        
        if sentences:
            # Pre-filter sentences to find only those with OOV words.
            interesting_sentences = filter_sentences_with_oov(sentences)

            # Delete old sentences for this URL before inserting new ones.
            supabase.table("sentence_queue").delete().eq("crawl_queue_id", url_id).execute()

            if interesting_sentences:
                # Insert only the interesting sentences into the database.
                supabase.table("sentence_queue").insert(
                    [{"crawl_queue_id": url_id, "sentence_text": s} for s in interesting_sentences]
                ).execute()

        # Update the crawl_queue status and metadata.
        supabase.table("crawl_queue").update({
            "extraction_status": "completed", "content_hash": new_hash,
            "last_modified": new_last_modified, "etag": new_etag,
            "processed_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", url_id).execute()
        return True

    except Exception as e:
        print(f"  [!] Preprocessing error: {url} - {e}", file=sys.stderr)
        supabase.table("crawl_queue").update({"extraction_status": "failed", "error_message": str(e)}).eq("id", url_id).execute()
        return False

# --- Main process orchestrator ---
def main():
    config = configparser.ConfigParser(); config.read('config.ini')
    max_workers = config.getint('Processor', 'MAX_WORKERS')
    batch_size = config.getint('Processor', 'PROCESS_BATCH_SIZE')
    req_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    safe_byte_limit = config.getint('Preprocessor', 'SAFE_BYTE_LIMIT')
    char_chunk_size = config.getint('Preprocessor', 'CHAR_CHUNK_SIZE')
    
    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    supabase = create_client(supabase_url, supabase_key)
    print("--- Text Extraction Process Started (with OOV Pre-filtering) ---")

    while True:
        res = supabase.table("crawl_queue").select("id, url, content_hash, last_modified, etag").eq("extraction_status", "queued").limit(batch_size).execute()
        if not res.data:
            print("[*] No URLs to preprocess in queue. Exiting.")
            break
        
        ids = [item['id'] for item in res.data]
        supabase.table("crawl_queue").update({"extraction_status": "processing"}).in_("id", ids).execute()
        print(f"[*] Locked {len(res.data)} URLs for extraction.")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(worker_preprocess_url, item, supabase_url, supabase_key, req_timeout, safe_byte_limit, char_chunk_size) for item in res.data]
            results = [f.result() for f in futures]
            success_count = sum(1 for r in results if r)
            print(f"  [+] Batch complete (Success: {success_count}, Fail: {len(results) - success_count})")
    
    print("--- Text Extraction Process Finished ---")

if __name__ == "__main__":
    main()