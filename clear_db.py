# clear_db.py
import os
import sys
import psycopg2
from supabase import create_client

def main():
    """
    Supabaseのテーブルから、stop_wordsとユーザー辞書以外のデータをすべて削除する。
    """
    print("[*] Clearing database tables (crawl_queue, unique_words, word_occurrences)...")
    
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        raise ValueError("Supabase credentials not set in environment.")

    supabase = create_client(supabase_url, supabase_key)
    
    # ▼▼▼▼▼ この一行を修正 ▼▼▼▼▼
    # supabase.functions.client から supabase.functions._client に変更
    db_info = supabase.functions._client.options.get("db", {})
    # ▲▲▲▲▲ ここまで修正 ▲▲▲▲▲

    db_url = db_info.get("url")

    if not db_url:
        raise ValueError("Could not retrieve the direct DB connection URL from Supabase client.")

    # TRUNCATE ... CASCADE を使い、依存関係のあるテーブルをまとめてクリア
    sql_command = """
    TRUNCATE TABLE
      public.unique_words,
      public.crawl_queue
    RESTART IDENTITY CASCADE;
    """
    
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                print("[*] Executing TRUNCATE command...")
                cur.execute(sql_command)
                conn.commit()
        print("[+] Database tables cleared successfully.")
    except Exception as e:
        print(f"[!!!] Failed to clear database: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
