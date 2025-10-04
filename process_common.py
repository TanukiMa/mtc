# process_common.py
import time
import datetime
import configparser
import sys
from sqlalchemy import func
from db_utils import get_local_db_session, SentenceQueue

def run_processor(processor_name, model_loader_func, batch_processor_func, db_status_column):
    """
    NLP処理の共通実行エンジン

    Args:
        processor_name (str): ログ出力用のプロセッサ名 (e.g., "GiNZA")
        model_loader_func (function): NLPモデルをロードする関数
        batch_processor_func (function): 文章のバッチを処理する関数
        db_status_column: 処理対象のステータスを示すDBカラム (e.g., SentenceQueue.ginza_status)
    """
    config = configparser.ConfigParser()
    config.read('config.ini')

    # --- 時間監視機能の実装 ---
    duration_minutes = config.getint('Processor', 'SAFE_RUN_DURATION_MINUTES', fallback=0)
    start_time = time.time()
    end_time = start_time + duration_minutes * 60 if duration_minutes > 0 else float('inf')
    
    print(f"--- [{processor_name}] Process Started ---")
    if duration_minutes > 0:
        print(f"[*] This process will run for a maximum of {duration_minutes} minutes.")

    # --- パフォーマンス改善：モデルは最初に一度だけロード ---
    print(f"[*] Loading {processor_name} model...")
    nlp_model = model_loader_func()
    print(f"[+] {processor_name} model loaded.")

    batch_size = config.getint('Processor', 'BATCH_SIZE', fallback=100)
    
    session = get_local_db_session()
    try:
        total_processed_count = 0
        while True:
            # --- 時間監視：ループの最初に残り時間を確認 ---
            if time.time() > end_time:
                print(f"\n[*] Time limit of {duration_minutes} minutes reached. Exiting gracefully.")
                break

            # --- パフォーマンス改善：バッチでDBから取得 ---
            # with_for_update(skip_locked=True)で並列実行時に他プロセスが処理中の行をスキップ
            items_to_process = session.query(SentenceQueue).filter(
                db_status_column == 'queued'
            ).order_by(
                SentenceQueue.id
            ).limit(
                batch_size
            ).with_for_update(skip_locked=True).all()

            if not items_to_process:
                print("\n[*] No more sentences to process. Exiting.")
                break

            ids_to_process = [item.id for item in items_to_process]
            sentences_to_process = [item.sentence_text for item in items_to_process]

            # 取得したバッチを「処理中」に更新してロック
            session.query(SentenceQueue).filter(
                SentenceQueue.id.in_(ids_to_process)
            ).update({"status": "processing"}, synchronize_session=False)
            session.commit()
            
            # --- NLPライブラリ固有のバッチ処理を実行 ---
            try:
                batch_processor_func(sentences_to_process, nlp_model)
                # ここで解析結果（単語など）を別テーブルに保存するロジックを追加可能
                
                # 処理が成功した行を「完了」に更新
                session.query(SentenceQueue).filter(
                    SentenceQueue.id.in_(ids_to_process)
                ).update({db_status_column.name: "completed"}, synchronize_session=False)

            except Exception as e:
                print(f"\n[!] Error processing batch: {e}", file=sys.stderr)
                # エラーが発生した行を「失敗」に更新
                session.query(SentenceQueue).filter(
                    SentenceQueue.id.in_(ids_to_process)
                ).update({db_status_column.name: "failed"}, synchronize_session=False)

            session.commit()
            total_processed_count += len(items_to_process)
            print(f"\r[*] Processed batches: {total_processed_count // batch_size} ({total_processed_count} sentences)", end="")

    except Exception as e:
        print(f"\n[!] A critical error occurred in {processor_name} processor: {e}", file=sys.stderr)
        if session.is_active:
            session.rollback()
    finally:
        session.close()
        print(f"\n--- [{processor_name}] Process Finished ---")