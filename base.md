github runners上でwebクローラー+日本語形態素解析して、新語を探す作業をしたい。可能かな？
対象は厚労省のサイト。専門用語。既存辞書と照合。実行頻度は定期実行。
一度解析したURLはpassするが、一定時間でリセット。
HTMLだけではなく、pptx pdf docxも対象にする
並列処理可能に
形態素解析には mecabではなく、Rustで書かれたSudachiを直接使いたい。
pip install supabase することで、supabaseと直接やり取りするのがスマートではないか？
supabaseのアカウント、パスワードは gh secretで保持。

車輪の再発明はせず、Ubuntu package、pip を利用してコードは短く。
sudachiはfull辞書を使いたい。

supabase用sql,table, コード、Github workflow fileを提案して下さい。
