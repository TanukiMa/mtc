# process_common.py (修正後の完全なコード)
import time
import datetime
import configparser
import sys
from sqlalchemy.dialects.postgresql import insert
from db_utils import get_local_db_session, SentenceQueue, UniqueWord

def run_processor(processor_name, model_loader_func, batch_processor_func, db_status_column):
    config = configparser.ConfigParser()
    config.read('config.ini')

    duration_minutes = config.getint('Processor', 'SAFE_RUN_DURATION_MINUTES', fallback=0)
    start_time = time.time()
    end_time = start_time + duration_minutes * 60 if duration_minutes > 0 else float('inf')
    
    print(f"--- [{processor_name}] Process Started ---")
    if duration_minutes > 0:
        print(f"[*] This process will run for a maximum of {duration_minutes} minutes.")

    print(f"[*] Loading {processor_name} model...")
    nlp_model = model_loader_func()
    print(f"[+] {processor_name} model loaded.")

    batch_size = config.getint('Processor', 'BATCH_SIZE', fallback=100)
    
    session = get_local_db_session()
    try:
        total_processed_count = 0
        while True:
            if time.time() > end_time:
                print(f"\n[*] Time limit reached. Exiting gracefully.")
                break

            items_to_process = session.query(SentenceQueue).filter(
                db_status_column == 'queued'
            ).order_by(SentenceQueue.id).limit(batch_size).with_for_update(skip_locked=True).all()

            if not items_to_process:
                print("\n[*] No more sentences to process. Exiting.")
                break

            ids_to_process = [item.id for item in items_to_process]
            sentences_to_process = [item.sentence_text for item in items_to_process]

            session.query(SentenceQueue).filter(
                SentenceQueue.id.in_(ids_to_process)
            ).update({db_status_column.name: "processing"}, synchronize_session=False)
            session.commit()
            
            try:
                # --- 修正点 1: batch_processor_funcから単語リストを受け取る ---
                discovered_words = batch_processor_func(sentences_to_process, nlp_model)
                
                # --- 修正点 2: 受け取った単語をDBに保存する ---
                if discovered_words:
                    # 'unique_word_per_tool'制約を利用して、重複を無視した挿入を行う
                    stmt = insert(UniqueWord).values(discovered_words).on_conflict_do_nothing(
                        index_elements=['word', 'source_tool']
                    )
                    session.execute(stmt)

                session.query(SentenceQueue).filter(
                    SentenceQueue.id.in_(ids_to_process)
                ).update({db_status_column.name: "completed"}, synchronize_session=False)

            except Exception as e:
                print(f"\n[!] Error processing batch: {e}", file=sys.stderr)
                session.rollback() # エラー時は単語もステータスも更新しない
                session.query(SentenceQueue).filter(
                    SentenceQueue.id.in_(ids_to_process)
                ).update({db_status_column.name: "failed"}, synchronize_session=False)

            session.commit()
            total_processed_count += len(items_to_process)
            print(f"\r[*] Processed {total_processed_count} sentences...", end="")

    except Exception as e:
        print(f"\n[!] A critical error occurred in {processor_name} processor: {e}", file=sys.stderr)
        if session.is_active:
            session.rollback()
    finally:
        session.close()
        print(f"\n--- [{processor_name}] Process Finished ---")