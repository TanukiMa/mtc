# process_ginza.py
import os, sys, re, time, configparser
from concurrent.futures import ProcessPoolExecutor
import spacy
import warnings

# SQLAlchemy関連のインポート
from sqlalchemy.dialects.postgresql import insert
from db_utils import get_local_db_session, CrawlQueue, SentenceQueue, UniqueWord, WordOccurrence, StopWord

warnings.filterwarnings("ignore", category=FutureWarning, module="huggingface_hub")

# --- Globals for worker processes ---
_NLP_MODEL = None

# --- Helper Functions ---
def analyze_with_ginza(text: str) -> list:
    """Uses GiNZA to extract named entities and other nouns from a given text."""
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
                if token.pos_ == "NOUN" and token.ent_type_ == "" and len(word_text) > 1 and word_text not in found_texts:
                     found_words.append({"word": word_text, "source_tool": "ginza", "entity_category": "NOUN_GENERAL", "pos_tag": token.tag_})
                     found_texts.add(word_text)
    except Exception as e:
        print(f"  [!] GiNZA analysis error: {e}", file=sys.stderr)
    return found_words

# --- Main worker function executed in each process ---
def worker_analyze_text(text_item_id, stop_words_set, db_write_chunk_size):
    """Analyzes a single text item from the local DB, finds new words, and saves them back to the local DB."""
    global _NLP_MODEL
    if _NLP_MODEL is None:
        _NLP_MODEL = spacy.load("ja_ginza_electra")

    session = get_local_db_session()
    try:
        # Fetch the item from the local DB
        text_item = session.query(SentenceQueue).options(joinedload(SentenceQueue.crawl_queue)).filter_by(id=text_item_id).one()
        source_url = text_item.crawl_queue.url
        text_to_analyze = text_item.sentence_text
        
        new_words = analyze_with_ginza(text_to_analyze)
        if new_words:
            sanitized_words = [w for w in (d.update({'word': d['word'].replace('\x00', '')}) or d for d in new_words) if w['word'] and w['word'] not in stop_words_set]
            if sanitized_words:
                # Upsert unique words using SQLAlchemy's ON CONFLICT support
                stmt = insert(UniqueWord).values(sanitized_words)
                stmt = stmt.on_conflict_do_nothing(index_elements=['word', 'source_tool'])
                session.execute(stmt)
                session.commit()

                # Get IDs of the words we just processed
                word_texts = [w['word'] for w in sanitized_words]
                word_records = session.query(UniqueWord.id, UniqueWord.word).filter(UniqueWord.word.in_(word_texts), UniqueWord.source_tool == 'ginza').all()
                word_to_id_map = {word: id for id, word in word_records}
                
                if word_to_id_map:
                    session.query(WordOccurrence).filter_by(source_url=source_url).delete(synchronize_session=False)
                    occurrences = [{"word_id": word_id, "source_url": source_url} for word, word_id in word_to_id_map.items()]
                    if occurrences:
                        occ_stmt = insert(WordOccurrence).values(occurrences)
                        occ_stmt = occ_stmt.on_conflict_do_nothing(index_elements=['word_id', 'source_url'])
                        session.execute(occ_stmt)

        text_item.ginza_status = 'completed'
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"  [!] GiNZA worker error on text ID {text_item_id}: {e}", file=sys.stderr)
        session.query(SentenceQueue).filter_by(id=text_item_id).update({"ginza_status": "failed"})
        session.commit()
    finally:
        session.close()

# --- Main process orchestrator ---
def main():
    config = configparser.ConfigParser(); config.read('config.ini')
    max_workers = config.getint('GiNZA_Processor', 'MAX_WORKERS')
    batch_size = config.getint('GiNZA_Processor', 'BATCH_SIZE')
    db_write_chunk_size = config.getint('GiNZA_Processor', 'DB_WRITE_CHUNK_SIZE')
    
    session = get_local_db_session()
    print("--- GiNZA Content Processor Started (Local DB Mode) ---")
    
    try:
        stop_words_records = session.query(StopWord.word).all()
        stop_words_set = {record.word for record in stop_words_records}
        print(f"[*] Loaded {len(stop_words_set)} stop words from local DB.")
    except Exception as e:
        print(f"[!] Could not load stop words from local DB: {e}", file=sys.stderr)
        stop_words_set = set()

    while True:
        # Fetch a batch of texts from the local DB
        items_to_process = session.query(SentenceQueue.id).filter_by(ginza_status='queued').limit(batch_size).all()
        if not items_to_process: 
            print("[*] No texts in queue for GiNZA to process. Exiting.")
            break
        
        ids_to_process = [item.id for item in items_to_process]
        
        # Lock the batch
        session.query(SentenceQueue).filter(SentenceQueue.id.in_(ids_to_process)).update({"ginza_status": "processing"}, synchronize_session=False)
        session.commit()
        print(f"[*] Locked {len(ids_to_process)} texts for GiNZA processing.")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(worker_analyze_text, item_id, stop_words_set, db_write_chunk_size) for item_id in ids_to_process]
            results = [f.result() for f in futures]
            # Success/Fail count is tricky here as workers return None on success, so we'll just log completion
            print(f"  [+] Batch of {len(futures)} tasks complete.")
            
    session.close()
    print("--- GiNZA Content Processor Finished ---")

if __name__ == "__main__":
    from sqlalchemy.orm import joinedload
    main()