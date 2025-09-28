# build_dict_source.py
import os
import configparser
from supabase import create_client, Client

def main():
    """Supabaseから全ユーザー辞書のデータを取得し、完全版(18列)フォーマットのCSVソースファイルを作成する"""
    
    supabase_url: str = os.environ.get("SUPABASE_URL")
    supabase_key: str = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: 
        raise ValueError("環境変数を設定してください。")

    supabase = create_client(supabase_url, supabase_key)
    
    dictionary_tables = ["general_user_dictionary", "medical_user_dictionary"]
    output_filename = "user_dict_source.csv"
    
    print(f"[*] ユーザー辞書ソースを '{output_filename}' に生成します...")
    
    total_words = 0
    with open(output_filename, 'w', encoding='utf-8') as f_out:
        for table in dictionary_tables:
            print(f"  [*] テーブル '{table}' からデータを取得中...")
            try:
                response = supabase.from_(table).select(
                    "surface, sudachi_reading, reading, pos_master(pos1, pos2, pos3, pos4, pos5, pos6)"
                ).execute()
                
                if not response.data:
                    print(f"    [-] データがありません。")
                    continue
                
                for item in response.data:
                    # ▼▼▼▼▼ 完全版(18列)フォーマットのCSV行を生成 ▼▼▼▼▼
                    surface = item['surface']
                    sudachi_reading = item['sudachi_reading']
                    reading = item['reading']
                    
                    pos_data = item.get('pos_master', {})
                    pos_parts = [
                        pos_data.get(f'pos{i}', '*') for i in range(1, 7)
                    ]
                    
                    # 18列の各要素を定義
                    # 1,2: 連接ID(空で自動計算), 3: コスト(-1で自動計算)
                    # 12: 正規化表記(表層形と同じ), 13-17: 未使用(*)
                    columns = [
                        surface,           # 0: 見出し (TRIE 用)
                        '',                # 1: 左連接ID
                        '',                # 2: 右連接ID
                        '-1',              # 3: コスト
                        sudachi_reading,   # 4: 見出し (表示用)
                        *pos_parts,        # 5-10: 品詞 (6要素)
                        reading,           # 11: 読み
                        surface,           # 12: 正規化表記
                        '*',               # 13: 辞書形ID
                        '*',               # 14: 分割タイプ
                        '*',               # 15: A単位分割情報
                        '*',               # 16: B単位分割情報
                        '*'                # 17: 未使用
                    ]
                    
                    line = ",".join(columns) + "\n"
                    f_out.write(line)
                    # ▲▲▲▲▲ ここまで修正 ▲▲▲▲▲
                
                print(f"    [+] {len(response.data)}件の単語を追加しました。")
                total_words += len(response.data)
            except Exception as e:
                print(f"    [!!!] テーブル '{table}' の処理中にエラー: {e}")

    print(f"\n[+] 合計 {total_words} 件の単語をCSVに出力しました。")

if __name__ == "__main__":
    main()
