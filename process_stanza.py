# process_stanza.py
import os
import sys
import re
import time
import configparser
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor

from bs4 import BeautifulSoup
from supabase import create_client, Client
import stanza

# --- Globals for worker processes ---
_NLP_MODEL = None

# --- Helper Functions ---
def analyze_with_stanza(text: str) -> list:
    """
    Uses Stanza to extract named entities and other nouns from a given text.
    """
    global _NLP_MODEL
    if not text.strip() or _NLP_MODEL is None:
        return []
    
    chunk_size = 50000 
    found_words = []
    found_texts = set()

    try:
        for i in range(0, len(text), chunk_size):
            chunk = text[i:i + chunk_size]
            doc = _NLP_MODEL(chunk)
            
            # 1. Extract Named Entities (excluding DATE)
            for ent in doc.ents:
                if ent.type != 'DATE':
                    word_text = ent.text.strip()
                    if len(word_text) > 1 and word_text not in found_texts:
                        found_words.append({
                            "word": word_text,
                            "source_tool": "stanza",
                            "entity_category": ent.type,
                            "pos_tag": "ENT"
                        })
                        found_texts.add(word_text)

            # 2. Extract other Nouns
            for sentence in doc.sentences:
                for word in sentence.words:
                    word_text = word.text.strip()
                    if word.upos == "NOUN" and len(word_text) > 1 and word_text not in found_texts:
                        found_words.append({
                            "word": word_text,
                            "source_tool": "stanza",
                            "entity_category": "NOUN_GENERAL",
                            "pos_tag": word.xpos
                        })
                        found_texts.add(word_text)
    except Exception as e:
        print(f"  [!] Stanza analysis error: {e}", file=sys.stderr)
    return found_words

# --- Main worker function executed in each process ---
def worker_analyze_text(text_item, supabase_url, supabase_key, stop_words_set):
    """
    Analyzes a single text item, finds new words, and saves them to the database.
    """
    global _NLP_MODEL
    if _NLP_MODEL is None:
        print(f"[*] Worker (PID: {os.getpid()}) loading pre-downloaded Stanza model 'ja'...")
        _NLP_MODEL = stanza.Pipeline('ja', verbose=False, use_gpu=False)

    text_id = text_item['id']
    crawl_queue_id = text_item['crawl_queue']['id']
    source_url = text_item['crawl_queue']['url']
    text_to_analyze = text_item['extracted_text']
    
    supabase: Client = create_client(supabase_url, supabase_key)

    try:
        new_words = analyze_with_stanza(text_to_analyze)
        
        if new_words:
            filtered_words = [w for w in new_words if w["word"] not in stop_words_set]

            if filtered_words:
                upsert_res = supabase.table("unique_words").upsert(
                    filtered_words, 
                    on_conflict="word, source_tool"
                ).execute()

                if upsert_res.data:
                    word_texts = [w['word'] for w in filtered_words]
                    select_res = supabase.table("unique_words").select("id, word").in_("word", word_texts).eq("source_tool", "stanza").execute()
                    
                    word_to_id_map = {item['word']: item['id'] for item in select_res.data}

                    if word_to_id_map:
                        supabase.table("word_occurrences").delete().eq("source_url", source_url).execute()
                        
                        occurrences = [{"word_id": word_id, "source_url": source_url} for word, word_id in word_to_id_map.items() if word in word_to_id_map]
                        if occurrences:
                            supabase.table("word_occurrences").insert(occurrences).execute()
        
        supabase.table("processed_texts").update({"stanza_status": "completed"}).eq("id", text_id).execute()
        return True

    except Exception as e:
        print(f"  [!] Stanza worker error on text ID {text_id}: {e}", file=sys.stderr)
        supabase.table("processed_texts").update({"stanza_status": "failed"}).eq("id", text_id).execute()
        return False

# --- Main process orchestrator ---
def main():
    config = configparser.ConfigParser(); config.read('config.ini')
    max_workers = config.getint('Processor', 'MAX_WORKERS')
    batch_size = config.getint('Processor', 'PROCESS_BATCH_SIZE')
    
    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    supabase = create_client(supabase_url, supabase_key)
    print("--- Stanza Content Processor Started ---")

    response = supabase.table("stop_words").select("word").execute()
    stop_words_set = {item['word'] for item in response.data}
    print(f"[*] Loaded {len(stop_words_set)} stop words.")
    
    while True:
        # Fetch a batch of texts that have not been processed by Stanza.
        res = supabase.table("processed_texts").select("id, extracted_text, crawl_queue(id, url)").eq("stanza_status", "queued").limit(batch_size).execute()
        
        if not res.data: 
            print("[*] No texts in queue for Stanza to process. Exiting.")
            break
        
        ids = [item['id'] for item in res.data]
        supabase.table("processed_texts").update({"stanza_status": "processing"}).in_("id", ids).execute()
        print(f"[*] Locked {len(res.data)} texts for Stanza processing.")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(worker_analyze_text, item, supabase_url, supabase_key, stop_words_set) for item in res.data]
            results = [f.result() for f in futures]
            success_count = sum(1 for r in results if r)
            print(f"  [+] Batch complete (Success: {success_count}, Fail: {len(results) - success_count})")
            
    print("--- Stanza Content Processor Finished ---")

if __name__ == "__main__":
    main()
