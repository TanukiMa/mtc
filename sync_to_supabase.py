import os
import configparser
from db_utils import (
    get_local_db_session, 
    get_supabase_client, 
    CrawlQueue, 
    SentenceQueue, 
    UniqueWord, 
    WordOccurrence,
    ProcessStatusEnum # <-- 修正点: 正しい名前に変更
)

def sync_table(local_session, supabase_client, model_class, table_name, columns, batch_size=500):
    """
    指定されたテーブルのデータをローカルからSupabaseへ同期する共通関数
    """
    print(f"[*] Syncing table: {table_name}...")
    
    total_rows = local_session.query(model_class).count()
    if total_rows == 0:
        print(f"   [-] No data to sync for {table_name}.")
        return

    print(f"   [+] Found {total_rows} rows to sync for {table_name}.")
    
    offset = 0
    while offset < total_rows:
        # 1. ローカルDBからバッチでデータを取得
        local_records = local_session.query(model_class).offset(offset).limit(batch_size).all()
        
        # 2. Supabaseに投入するデータ形式 (dictのリスト) に変換
        data_to_upsert = []
        for record in local_records:
            row_dict = {}
            for col in columns:
                value = getattr(record, col)
                # Enum型を文字列に変換
                if isinstance(value, ProcessStatusEnum):
                    row_dict[col] = str(value.name)
                else:
                    row_dict[col] = value
            data_to_upsert.append(row_dict)

        # 3. SupabaseへUpsert (存在すれば更新、なければ挿入)
        if data_to_upsert:
            try:
                supabase_client.table(table_name).upsert(data_to_upsert).execute()
                print(f"\r   [->] Synced {offset + len(local_records)} / {total_rows} rows...", end="")
            except Exception as e:
                print(f"\n   [!] Error during upsert to {table_name}: {e}")
                # エラーが発生しても処理を続行する場合もあるが、ここでは停止
                return
        
        offset += batch_size
    
    print(f"\n   [+] Successfully synced table: {table_name}.")


def main():
    """
    ローカルDBの全データをSupabaseに同期する
    """
    print("--- Sync to Supabase Started ---")
    local_session = get_local_db_session()
    supabase = get_supabase_client()

    try:
        # 同期対象のテーブルとカラムを定義
        tables_to_sync = {
            "crawl_queue": (CrawlQueue, ["id", "url", "extraction_status", "content_hash", "last_modified", "etag", "processed_at"]),
            "sentence_queue": (SentenceQueue, ["id", "crawl_queue_id", "sentence_text", "ginza_status", "stanza_status"]),
            "unique_words": (UniqueWord, ["id", "word", "source_tool", "entity_category", "pos_tag", "discovered_at"]),
            "word_occurrences": (WordOccurrence, ["id", "word_id", "source_url"]),
        }

        for table_name, (model, cols) in tables_to_sync.items():
            sync_table(local_session, supabase, model, table_name, cols)

    except Exception as e:
        print(f"\n[!] A critical error occurred during sync process: {e}")
    finally:
        local_session.close()

    print("--- Sync to Supabase Finished ---")

if __name__ == "__main__":
    main()