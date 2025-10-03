# sync_to_supabase.py
import os
import sys
import configparser
from sqlalchemy.orm import joinedload
from sqlalchemy import func, case
from supabase import create_client, Client

# Import the necessary components from our db_utils
from db_utils import get_local_db_session, CrawlQueue, SentenceQueue, UniqueWord, WordOccurrence, StopWord, ProcessStatus

def main():
    """
    Syncs the final state from the local PostgreSQL database to the remote Supabase database.
    This script is the final source of truth for determining completion status.
    """
    print("[*] Syncing final results from local DB to Supabase...")
    
    local_session = get_local_db_session()
    
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise ValueError("Supabase credentials are not set in the environment.")
    supabase = create_client(supabase_url, supabase_key)
    
    try:
        # 1. Sync unique_words
        unique_words = local_session.query(UniqueWord).all()
        if unique_words:
            print(f"  [+] Syncing {len(unique_words)} unique words...")
            data = [{"word": w.word, "source_tool": w.source_tool, "entity_category": w.entity_category, "pos_tag": w.pos_tag} for w in unique_words]
            # Upsert in chunks
            chunk_size = 500
            for i in range(0, len(data), chunk_size):
                supabase.table("unique_words").upsert(data[i:i + chunk_size], on_conflict="word, source_tool").execute()

        # 2. Sync word_occurrences
        print("[*] Syncing word occurrences...")
        all_words_in_db = supabase.table("unique_words").select("id, word, source_tool").execute().data
        word_map = {(w['word'], w['source_tool']): w['id'] for w in all_words_in_db}
        
        occurrences = local_session.query(WordOccurrence).options(joinedload(WordOccurrence.unique_word)).all()
        if occurrences:
            occ_data_to_insert = []
            for occ in occurrences:
                word_key = (occ.unique_word.word, occ.unique_word.source_tool)
                if word_key in word_map:
                    occ_data_to_insert.append({"word_id": word_map[word_key], "source_url": occ.source_url})
            
            if occ_data_to_insert:
                print(f"  [+] Syncing {len(occ_data_to_insert)} word occurrences...")
                # Clear and replace is simpler than diffing for occurrences
                supabase.table("word_occurrences").delete().neq("id", 0).execute()
                chunk_size = 500
                for i in range(0, len(occ_data_to_insert), chunk_size):
                    supabase.table("word_occurrences").insert(occ_data_to_insert[i:i + chunk_size]).execute()

        # 3. Sync crawl_queue with CORRECT final status
        print("[*] Determining final URL statuses and syncing crawl_queue...")
        
        # Check which crawl_queue IDs have fully completed NLP tasks
        # This subquery counts how many sentences are NOT completed for each URL
        incomplete_subquery = local_session.query(
            SentenceQueue.crawl_queue_id,
            func.count(SentenceQueue.id).label("incomplete_count")
        ).filter(
            (SentenceQueue.ginza_status != ProcessStatus.completed) |
            (SentenceQueue.stanza_status != ProcessStatus.completed)
        ).group_by(SentenceQueue.crawl_queue_id).subquery()

        # Join crawl_queue with the subquery to determine final status
        crawl_queue_items = local_session.query(
            CrawlQueue,
            incomplete_subquery.c.incomplete_count
        ).outerjoin(
            incomplete_subquery,
            CrawlQueue.id == incomplete_subquery.c.crawl_queue_id
        ).all()

        if crawl_queue_items:
            data_to_upsert = []
            for cq, incomplete_count in crawl_queue_items:
                # If there are any incomplete sentences, reset status to 'queued' for next run
                final_status = ProcessStatus.completed if incomplete_count is None or incomplete_count == 0 else ProcessStatus.queued
                data_to_upsert.append({
                    "url": cq.url,
                    "extraction_status": final_status.name,
                    "content_hash": cq.content_hash,
                    "last_modified": cq.last_modified,
                    "etag": cq.etag,
                    "processed_at": cq.processed_at
                })

            print(f"  [+] Syncing {len(data_to_upsert)} URL states to Supabase...")
            chunk_size = 500
            for i in range(0, len(data_to_upsert), chunk_size):
                supabase.table("crawl_queue").upsert(data_to_upsert[i:i + chunk_size], on_conflict="url").execute()

    except Exception as e:
        print(f"[!!!] An error occurred during sync to Supabase: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        local_session.close()

    print("\n--- Sync to Supabase Finished ---")

if __name__ == "__main__":
    main()