# process_stanza.py
import os, sys, re, time, configparser
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor
from supabase import create_client, Client
import stanza
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="torch._weights_only_unpickler")

_NLP_MODEL = None

def analyze_with_stanza(text: str) -> list:
    global _NLP_MODEL
    if not text.strip() or _NLP_MODEL is None: return []
    chunk_size = 50000 
    found_words, found_texts = [], set()
    try:
        for i in range(0, len(text), chunk_size):
            chunk = text[i:i + chunk_size]
            doc = _NLP_MODEL(chunk)
            date_texts = {ent.text.strip() for ent in doc.ents if ent.type == 'DATE'}
            for ent in doc.ents:
                if ent.type != 'DATE':
                    word_text = ent.text.strip()
                    if len(word_text) > 1 and word_text not in found_texts:
                        found_words.append({"word": word_text, "source_tool": "stanza", "entity_category": ent.type, "pos_tag": "ENT"})
                        found_texts.add(word_text)
            for sentence in doc.sentences:
                for word in sentence.words:
                    word_text = word.text.strip()
                    if word.upos == "NOUN" and len(word_text) > 1 and word_text not in found_texts and word_text not in date_texts:
                        found_words.append({"word": word_text, "source_tool": "stanza", "entity_category": "NOUN_GENERAL", "pos_tag": word.xpos})
                        found_texts.add(word_text)
    except Exception as e:
        print(f"  [!] Stanza analysis error: {e}", file=sys.stderr)
    return found_words

def worker_analyze_text(text_item, supabase_url, supabase_key, stop_words_set):
    global _NLP_MODEL
    if _NLP_MODEL is None:
        _NLP_MODEL = stanza.Pipeline('ja', verbose=False, use_gpu=False)

    text_id, crawl_queue_id, text_to_analyze = text_item['id'], text_item['crawl_queue_id'], text_item['sentence_text']
    supabase: Client = create_client(supabase_url, supabase_key)
    try:
        url_res = supabase.table("crawl_queue").select("url").eq("id", crawl_queue_id).single().execute()
        source_url = url_res.data['url'] if url_res.data else f"unknown_url_for_crawl_id_{crawl_queue_id}"
        new_words = analyze_with_stanza(text_to_analyze)
        if new_words:
            sanitized_words = []
            for word_data in new_words:
                word_text = word_data["word"]
                if word_text not in stop_words_set:
                    word_data['word'] = word_text.replace('\x00', '')
                    if word_data['word']:
                        sanitized_words.append(word_data)
            if sanitized_words:
                upsert_res = supabase.table("unique_words").upsert(sanitized_words, on_conflict="word, source_tool").execute()
                if upsert_res.data:
                    word_texts = [w['word'] for w in sanitized_words]
                    select_res = supabase.table("unique_words").select("id, word").in_("word", word_texts).eq("source_tool", "stanza").execute()
                    word_to_id_map = {item['word']: item['id'] for item in select_res.data}
                    if word_to_id_map:
                        supabase.table("word_occurrences").delete().eq("source_url", source_url).execute()
                        occurrences = [{"word_id": word_id, "source_url": source_url} for word, word_id in word_to_id_map.items() if word in word_to_id_map]
                        if occurrences:
                            supabase.table("word_occurrences").upsert(occurrences, on_conflict="word_id, source_url").execute()
        supabase.table("sentence_queue").update({"stanza_status": "completed"}).eq("id", text_id).execute()
        return True
    except Exception as e:
        print(f"  [!] Stanza worker error on text ID {text_id}: {e}", file=sys.stderr)
        supabase.table("sentence_queue").update({"stanza_status": "failed"}).eq("id", text_id).execute()
        return False

def main():
    config = configparser.ConfigParser(); config.read('config.ini')
    # ▼▼▼▼▼ [Stanza_Processor]から設定を読み込む ▼▼▼▼▼
    max_workers = config.getint('Stanza_Processor', 'MAX_WORKERS')
    batch_size = config.getint('Stanza_Processor', 'BATCH_SIZE')
    # ▲▲▲▲▲ ここまで修正 ▲▲▲▲▲
    
    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    supabase = create_client(supabase_url, supabase_key)
    print("--- Stanza Content Processor Started ---")

    response = supabase.table("stop_words").select("word").execute()
    stop_words_set = {item['word'] for item in response.data}
    print(f"[*] Loaded {len(stop_words_set)} stop words.")
    
    max_retries = 3
    while True:
        res = None
        for attempt in range(max_retries):
            try:
                res = supabase.table("sentence_queue").select("id, sentence_text, crawl_queue_id").eq("stanza_status", "queued").limit(batch_size).execute()
                break
            except Exception as e:
                if attempt < max_retries - 1: time.sleep(5 * (attempt + 1))
                else: print("[!!!] DB query failed. Exiting.", file=sys.stderr); return
        
        if not res or not res.data: 
            print("[*] No texts in queue for Stanza to process. Exiting.")
            break
        
        ids = [item['id'] for item in res.data]
        try:
            supabase.table("sentence_queue").update({"stanza_status": "processing"}).in_("id", ids).execute()
            print(f"[*] Locked {len(res.data)} texts for Stanza processing.")
        except Exception as e:
            print(f"  [!] DB lock failed: {e}", file=sys.stderr); time.sleep(5); continue

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(worker_analyze_text, item, supabase_url, supabase_key, stop_words_set) for item in res.data]
            results = [f.result() for f in futures]
            success_count = sum(1 for r in results if r)
            print(f"  [+] Batch complete (Success: {success_count}, Fail: {len(results) - success_count})")
            
    print("--- Stanza Content Processor Finished ---")

if __name__ == "__main__":
    main()