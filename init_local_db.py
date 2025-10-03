# init_local_db.py
from db_utils import get_local_db_session, get_supabase_client, Base, CrawlQueue, StopWord
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert

def main():
    print("[*] Initializing local PostgreSQL database...")
    engine = create_engine(os.environ["LOCAL_DB_URL"])
    
    # 1. Create all tables in the local DB
    Base.metadata.create_all(engine)
    print("  [+] Local tables created.")

    # 2. Fetch initial data from Supabase
    supabase = get_supabase_client()
    session = get_local_db_session()
    print("[*] Fetching initial data from Supabase...")

    # Fetch and load crawl_queue
    all_crawl_data = []
    page = 0
    while True:
        res = supabase.table("crawl_queue").select("*").range(page * 1000, (page + 1) * 1000 - 1).execute()
        if not res.data: break
        all_crawl_data.extend(res.data)
        page += 1
    
    if all_crawl_data:
        print(f"  [+] Loading {len(all_crawl_data)} URLs from previous run into local DB...")
        session.bulk_insert_mappings(CrawlQueue, all_crawl_data)
        session.commit()

    # Fetch and load stop_words
    stop_words = supabase.table("stop_words").select("*").execute().data
    if stop_words:
        print(f"  [+] Loading {len(stop_words)} stop words to local DB...")
        session.bulk_insert_mappings(StopWord, stop_words)
        session.commit()
    
    session.close()
    print("[*] Local database initialization complete.")

if __name__ == "__main__":
    main()