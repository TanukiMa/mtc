# init_local_db.py
import os
from sqlalchemy import create_engine

# 修正されたdb_utilsから、Baseと同期対象の全モデルをインポートする
from db_utils import (
    get_local_db_session, 
    get_supabase_client, 
    Base, 
    CrawlQueue, 
    StopWord, 
    BoilerplatePattern # BoilerplatePatternモデルを追加
)

def main():
    """
    ローカルのPostgreSQLデータベースを初期化し、Supabaseから初期データを同期する
    """
    print("[*] Initializing local PostgreSQL database...")
    
    db_url = os.environ.get("LOCAL_DB_URL")
    if not db_url:
        print("[!] Error: Environment variable LOCAL_DB_URL is not set.")
        return
        
    engine = create_engine(db_url)
    
    # 1. db_utilsで定義された全てのテーブルをローカルDBに作成
    #    (存在しないテーブルのみ作成される)
    Base.metadata.create_all(engine)
    print("   [+] Local tables created (if not exist).")

    # 2. Supabaseから初期データを取得してローカルに同期
    try:
        supabase = get_supabase_client()
        session = get_local_db_session()
        print("[*] Fetching initial data from Supabase...")

        # crawl_queueテーブルのデータを同期
        all_crawl_data = []
        page = 0
        while True:
            res = supabase.table("crawl_queue").select("id, url, extraction_status, content_hash, last_modified, etag, processed_at").range(page * 1000, (page + 1) * 1000 - 1).execute()
            if not res.data:
                break
            all_crawl_data.extend(res.data)
            page += 1
        
        if all_crawl_data:
            print(f"   [+] Loading {len(all_crawl_data)} URLs from Supabase into local DB...")
            # 既存のデータをクリアしてから挿入（멱등성을担保するため）
            session.query(CrawlQueue).delete(synchronize_session=False)
            session.bulk_insert_mappings(CrawlQueue, all_crawl_data)
            session.commit()
        else:
            print("   [-] No crawl_queue data found in Supabase.")

        # stop_wordsテーブルのデータを同期
        stop_words = supabase.table("stop_words").select("id, word, reason, created_at").execute().data
        if stop_words:
            print(f"   [+] Loading {len(stop_words)} stop words to local DB...")
            session.query(StopWord).delete(synchronize_session=False)
            session.bulk_insert_mappings(StopWord, stop_words)
            session.commit()
        else:
            print("   [-] No stop_words data found in Supabase.")

        # boilerplate_patternsテーブルのデータを同期 (--- 追加部分 ---)
        boilerplate_patterns = supabase.table("boilerplate_patterns").select("id, pattern, reason, created_at").execute().data
        if boilerplate_patterns:
            print(f"   [+] Loading {len(boilerplate_patterns)} boilerplate patterns to local DB...")
            session.query(BoilerplatePattern).delete(synchronize_session=False)
            session.bulk_insert_mappings(BoilerplatePattern, boilerplate_patterns)
            session.commit()
        else:
            print("   [-] No boilerplate_patterns data found in Supabase.")
        # (--- 追加部分ここまで ---)
            
    except Exception as e:
        print(f"[!] An error occurred during synchronization: {e}")
        if 'session' in locals() and session.is_active:
            session.rollback()
    finally:
        if 'session' in locals():
            session.close()

    print("[*] Local database initialization complete.")

if __name__ == "__main__":
    main()