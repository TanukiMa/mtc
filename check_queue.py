# check_queue.py
import os
from supabase import create_client, Client

def main():
    """
    sentence_queueテーブルを調べ、GiNZAとStanzaが処理すべき
    それぞれの件数を数えて、GitHub Actionsの出力として設定する。
    """
    supabase_url: str = os.environ.get("SUPABASE_URL")
    supabase_key: str = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: 
        raise ValueError("Supabase credentials not set in environment.")

    supabase = create_client(supabase_url, supabase_key)
    
    # GiNZAの未処理件数をカウント
    ginza_res = supabase.table("sentence_queue").select("id", count='exact').eq("ginza_status", "queued").execute()
    ginza_count = ginza_res.count if ginza_res else 0
    print(f"Sentences to process for GiNZA: {ginza_count}")

    # Stanzaの未処理件数をカウント
    stanza_res = supabase.table("sentence_queue").select("id", count='exact').eq("stanza_status", "queued").execute()
    stanza_count = stanza_res.count if stanza_res else 0
    print(f"Sentences to process for Stanza: {stanza_count}")

    # GitHub Actionsの次のステップで使えるように、結果を出力変数に設定
    if 'GITHUB_OUTPUT' in os.environ:
        with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
            print(f'ginza_count={ginza_count}', file=f)
            print(f'stanza_count={stanza_count}', file=f)

if __name__ == "__main__":
    main()