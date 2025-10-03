# sync_to_supabase.py
import os
import sys
import configparser
from sqlalchemy.orm import joinedload
from supabase import create_client, Client

# Import the necessary components from our db_utils
from db_utils import get_local_db_session, CrawlQueue, UniqueWord, WordOccurrence

def main():
    """
    Syncs the final state from the local PostgreSQL database to the remote Supabase database.
    """
    print("[*] Syncing final results from local DB to Supabase...")
    
    local_session = get_local_db_session()
    
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise ValueError("Supabase credentials are not set in the environment.")
    supabase = create_client(supabase_url, supabase_key)
    
    try:
        # 1. Sync the crawl_queue table
        crawl_queue_items = local_session.query(CrawlQueue).all()
        if crawl_queue_items:
            print(f"  [+] Syncing {len(crawl_queue_items)} URL states to Supabase...")
            # Convert SQLAlchemy objects to a list of dictionaries
            data_to_upsert = [
                {
                    "url": item.url,
                    "extraction_status": item.extraction_status.name,
                    "content_hash": item.content_hash,
                    "last_modified": item.last_modified,
                    "etag": item.etag,
                    "processed_at": item.processed_at.isoformat() if item.processed_at else None
                } 
                for item in crawl_queue_items
            ]
            
            # Upsert in chunks to avoid request size limits
            chunk_size = 500
            for i in range(0, len(data_to_upsert), chunk_size):
                chunk = data_to_upsert[i:i + chunk_size]
                supabase.table("crawl_queue").upsert(chunk, on_conflict="url").execute()

        # 2. Sync the unique_words table
        unique_words = local_session.query(UniqueWord).all()
        if unique_words:
            print(f"  [+] Syncing {len(unique_words)} unique words to Supabase...")
            data_to_upsert = [
                {
                    "word": w.word, 
                    "source_tool": w.source_tool, 
                    "entity_category": w.entity_category, 
                    "pos_tag": w.pos_tag
                } 
                for w in unique_words
            ]
            chunk_size = 500
            for i in range(0, len(data_to_upsert), chunk_size):
                chunk = data_to_upsert[i:i + chunk_size]
                supabase.table("unique_words").upsert(chunk, on_conflict="word, source_tool").execute()

        # 3. Sync the word_occurrences table
        # This is more complex, so we'll do a clear and replace for simplicity.
        # A more advanced version might do a diff.
        print("[*] Syncing word occurrences...")
        
        # First, clear all previous occurrences in Supabase.
        supabase.table("word_occurrences").delete().neq("id", 0).execute()
        
        # Then, insert all from the local DB.
        occurrences = local_session.query(WordOccurrence).options(joinedload(WordOccurrence.unique_word)).all()
        if occurrences:
            # We need to get the IDs of the words as they exist in Supabase,
            # which requires an extra query, but for simplicity, we'll assume the IDs match for now.
            # A truly robust solution would re-fetch word IDs from Supabase after the unique_words sync.
            
            # Let's keep it simple for now by re-fetching the word map.
            all_words_in_db = supabase.table("unique_words").select("id, word, source_tool").execute().data
            word_map = {(w['word'], w['source_tool']): w['id'] for w in all_words_in_db}
            
            occ_data_to_insert = []
            for occ in occurrences:
                word_key = (occ.unique_word.word, occ.unique_word.source_tool)
                if word_key in word_map:
                    occ_data_to_insert.append({
                        "word_id": word_map[word_key],
                        "source_url": occ.source_url
                    })
            
            if occ_data_to_insert:
                print(f"  [+] Syncing {len(occ_data_to_insert)} word occurrences...")
                chunk_size = 500
                for i in range(0, len(occ_data_to_insert), chunk_size):
                    chunk = occ_data_to_insert[i:i + chunk_size]
                    supabase.table("word_occurrences").insert(chunk).execute()

    except Exception as e:
        print(f"[!!!] An error occurred during sync to Supabase: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        local_session.close()

    print("\n--- Sync to Supabase Finished ---")

if __name__ == "__main__":
    main()