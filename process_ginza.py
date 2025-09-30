# process_ginza.py
import os
import sys
import re
import time
import configparser
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor

from bs4 import BeautifulSoup
from supabase import create_client, Client
import spacy

_NLP_MODEL = None

def analyze_with_ginza(text: str) -> list:
    global _NLP_MODEL
    if not text.strip() or _NLP_MODEL is None: return []
    chunk_size = 40000 
    found_words = []
    found_texts = set()
    try:
        for i in range(0, len(text), chunk_size):
            chunk = text[i:i + chunk_size]
            doc = _NLP_MODEL(chunk)
            for ent in doc.ents:
                if ent.label_ != 'DATE':
                    word_text = ent.text.strip()
                    if len(word_text) > 1 and word_text not in found_texts:
                        found_words.append({"word": word_text, "source_tool": "ginza", "entity_category": ent.label_, "pos_tag": "ENT"})
                        found_texts.add(word_text)
            for token in doc:
                word_text = token.text.strip()
                if token.pos_ == "NOUN" and len(word_text) > 1 and word_text not in found_texts:
                     found_words.append({"word": word_text, "source_tool": "ginza", "entity_category": "NOUN_GENERAL", "pos_tag": token.tag_})
                     found_texts.add(word_text)
    except Exception as e:
        print(f"  [!] GiNZA analysis error: {e}", file=sys.stderr)
    return found_words

def worker_analyze_text(text_item, supabase_url, supabase_key, stop_words_set):
    global _NLP_MODEL
    if _NLP_MODEL is None:
        _NLP_MODEL = spacy.load("ja_ginza_electra")

    text_id, crawl_queue_id, text_to_analyze = text_item['id'], text_item['crawl_queue_id'], text_item['sentence_text']
    supabase = create_client(supabase_url, supabase_key)

    try:
        url_res = supabase.table("crawl_queue").select("url").eq("id", crawl_queue_id).single().execute()
        source_url = url_res.data['url'] if url_res.data else f"unknown_url_for_crawl_id_{crawl_queue_id}"

        new_words = analyze_with_ginza(text_to_analyze)
        if new_words:
            filtered_words = [w for w in new_words if w["word"] not in stop_words_set]
            if filtered_words:
                upsert_res = supabase.table("unique_words").upsert(filtered_words, on_conflict="word, source_tool").execute()
                if upsert_res.data:
                    word_texts = [w['word'] for w in filtered_words]
                    select_res = supabase.table("unique_words").select("id, word").in_("word", word_texts).eq("source_tool", "ginza").execute()
                    word_to_id_map = {item['word']: item['id'] for item in select_res.data}
                    if word_to_id_map:
                        supabase.table("word_occurrences").delete().eq("source_url", source_url).execute()
                        occurrences = [{"word_id": word_id, "source_url": source_url} for word, word_id in word_to_id_map.items() if word in word_to_id_map]
                        if occurrences:
                            supabase.table("word_occurrences").upsert(occurrences, on_conflict="word_id, source_url").execute()

        supabase.table("sentence_queue").update({"ginza_status": "completed"}).eq("id", text_id).execute()
        return True
    except Exception as e:
        print(f"  [!] GiNZA worker error on text ID {text_id}: {e}", file=sys.stderr)
        supabase.table("sentence_queue").update({"ginza_status": "failed"}).eq("id", text_id).execute()
        return False

def main():
    config = configparser.ConfigParser(); config.read('config.ini')
    max_workers = config.getint('Processor', 'MAX_WORKERS')
    batch_size = config.getint('Processor', 'PROCESS_BATCH_SIZE')
    
    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    supabase = create_client(supabase_url, supabase_key)
    print("--- GiNZA Content Processor Started ---")

    response = supabase.table("stop_words").select("word").execute()
    stop_words_set = {item['word'] for item in response.data}
    print(f"[*] Loaded {len(stop_words_set)} stop words.")

    while True:
        res = supabase.table("sentence_queue").select("id, sentence_text, crawl_queue_id").eq("ginza_status", "queued").limit(batch_size).execute()
        
        if not res.data: 
            print("[*] No texts in queue for GiNZA to process. Exiting.")
            break
        
        ids = [item['id'] for item in res.data]
        supabase.table("sentence_queue").update({"ginza_status": "processing"}).in_("id", ids).execute()
        print(f"[*] Locked {len(res.data)} texts for GiNZA processing.")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(worker_analyze_text, item, supabase_url, supabase_key, stop_words_set) for item in res.data]
            results = [f.result() for f in futures]
            success_count = sum(1 for r in results if r)
            print(f"  [+] Batch complete (Success: {success_count}, Fail: {len(results) - success_count})")
            
    print("--- GiNZA Content Processor Finished ---")

if __name__ == "__main__":
    main()
