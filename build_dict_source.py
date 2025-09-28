# build_dict_source.py
import os
import configparser
from supabase import create_client, Client

def main():
    """Supabaseから全ユーザー辞書のデータを取得し、単一のCSVソースファイルを作成する"""
    
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
                # pos_masterテーブルをJOINして、品詞文字列を取得
                response = supabase.from_(table).select(
                    "surface, sudachi_reading, reading, pos_master(pos1, pos2, pos3, pos4, pos5, pos6)"
                ).execute()
                
                if not response.data:
                    print(f"    [-] データがありません。")
                    continue
                
                for item in response.data:
                    pos_data = item.get('pos_master')
                    if not pos_data: continue
                    
                    pos_parts = [
                        pos_data.get(f'pos{i}', '*') for i in range(1, 7)
                    ]
                    pos_string = ",".join(p if p is not None else '*' for p in pos_parts)
                    line = f"{item['surface']},{item['sudachi_reading']},{item['reading']},{pos_string}\n"
                    f_out.write(line)
                
                print(f"    [+] {len(response.data)}件の単語を追加しました。")
                total_words += len(response.data)
            except Exception as e:
                print(f"    [!!!] テーブル '{table}' の処理中にエラー: {e}")

    print(f"\n[+] 合計 {total_words} 件の単語をCSVに出力しました。")

if __name__ == "__main__":
    main()
