# preprocess.py
import os, sys, re, time, configparser, requests, hashlib
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor
from bs4 import BeautifulSoup
from supabase import create_client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_sentences_from_html(content: bytes) -> list:
    sentences = []
    try:
        soup = BeautifulSoup(content, 'html.parser')
        for s in soup(["script", "style", "header", "footer", "nav"]): s.decompose()
        for element in soup.find_all(['p', 'h1', 'h2', 'h3', 'li', 'td', 'th']):
            text = re.sub(r'\s+', ' ', element.get_text(strip=True))
            if text: sentences.append(text)
        return sentences
    except Exception as e:
        raise RuntimeError(f"HTML parsing error: {e}")

def worker_preprocess_url(queue_item, supabase_url, supabase_key, request_timeout):
    url_id, url, old_hash, old_last_modified, old_etag = queue_item['id'], queue_item['url'], queue_item.get('content_hash'), queue_item.get('last_modified'), queue_item.get('etag')

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

        if (old_last_modified and old_last_modified == new_last_modified) or (old_etag and old_etag == new_etag):
            supabase.table("crawl_queue").update({"preprocess_status": "completed", "processed_at": datetime.now(timezone.utc).isoformat()}).eq("id", url_id).execute()
            return True

        response = session.get(url, timeout=request_timeout, headers=headers, allow_redirects=True)
        response.raise_for_status()
        new_hash = hashlib.sha256(response.content).hexdigest()

        if old_hash and old_hash == new_hash:
            supabase.table("crawl_queue").update({"preprocess_status": "completed", "last_modified": new_last_modified, "etag": new_etag, "processed_at": datetime.now(timezone.utc).isoformat()}).eq("id", url_id).execute()
            return True

        sentences = get_sentences_from_html(response.content)
        if sentences:
            supabase.table("sentence_queue").delete().eq("crawl_queue_id", url_id).execute()
            supabase.table("sentence_queue").insert([{"crawl_queue_id": url_id, "sentence_text": s} for s in sentences]).execute()

        supabase.table("crawl_queue").update({"preprocess_status": "completed", "content_hash": new_hash, "last_modified": new_last_modified, "etag": new_etag, "processed_at": datetime.now(timezone.utc).isoformat()}).eq("id", url_id).execute()
        return True

    except Exception as e:
        print(f"  [!] Preprocessing error: {url} - {e}", file=sys.stderr)
        supabase.table("crawl_queue").update({"preprocess_status": "failed", "error_message": str(e)}).eq("id", url_id).execute()
        return False

def main():
    config = configparser.ConfigParser(); config.read('config.ini')
    max_workers = config.getint('Processor', 'MAX_WORKERS')
    batch_size = config.getint('Processor', 'PROCESS_BATCH_SIZE')
    req_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    
    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    supabase = create_client(supabase_url, supabase_key)
    print("--- Preprocessing Stage Started ---")

    while True:
        res = supabase.table("crawl_queue").select("id, url, content_hash, last_modified, etag").eq("preprocess_status", "queued").limit(batch_size).execute()
        if not res.data:
            print("[*] No URLs to preprocess in queue. Exiting.")
            break
        
        ids = [item['id'] for item in res.data]
        supabase.table("crawl_queue").update({"preprocess_status": "processing"}).in_("id", ids).execute()
        print(f"[*] Locked {len(res.data)} URLs for preprocessing.")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(worker_preprocess_url, item, supabase_url, supabase_key, req_timeout) for item in res.data]
            results = [f.result() for f in futures]
            success_count = sum(1 for r in results if r)
            print(f"  [+] Batch complete (Success: {success_count}, Fail: {len(results) - success_count})")
    
    print("--- Preprocessing Stage Finished ---")

if __name__ == "__main__":
    main()
