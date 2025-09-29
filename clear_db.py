# clear_db.py
import os
import psycopg2
from supabase import create_client

def main():
    """
    Supabaseのテーブルから、stop_words以外のデータをすべて削除する。
    """
    print("[*] Clearing database tables (crawl_queue, unique_words, word_occurrences)...")
    
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        raise ValueError("Supabase credentials not set in environment.")

    # Supabaseクライアントを使用して、DBの接続情報を取得
    supabase = create_client(supabase_url, supabase_key)
    db_info = supabase.functions.client.options.get("db", {})
    db_url = db_info.get("url")

    if not db_url:
        raise ValueError("Could not retrieve the direct DB connection URL from Supabase client.")

    sql_command = """
    TRUNCATE TABLE
      public.word_occurrences,
      public.unique_words,
      public.crawl_queue
    RESTART IDENTITY;
    """
    
    try:
        # DBに直接接続してTRUNCATEを実行
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                print("[*] Executing TRUNCATE command...")
                cur.execute(sql_command)
                conn.commit()
        print("[+] Database tables cleared successfully.")
    except Exception as e:
        print(f"[!!!] Failed to clear database: {e}")
        # エラーが発生した場合は、ワークフローを失敗させる
        exit(1)

if __name__ == "__main__":
    main()
