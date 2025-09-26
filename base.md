github runners上でwebクローラー+日本語形態素解析して、新語を探す作業をしたい。可能かな？
対象は厚労省のサイト。専門用語。既存辞書と照合。実行頻度は定期実行。
一度解析したURLはpassする
HTMLだけではなく、pptx pdf docxも対象にする
並列処理可能に
形態素解析には mecabではなく、Javaで書かれたSudachiを直接使いたい。sudachipy は開発が停止しているので使用しない。その方針でmecabから Sudachiに形態素解析エンジンを変更したい。
pip install supabase することで、supabaseと直接やり取りするのがスマートではないか？
supabaseのアカウント、パスワードは gh secretで保持。
llama.cpp と適切なLLMを用いて、新語か否か判定する。

車輪の再発明はせず、Ubuntu package、pip を利用してコードは短く。

supabase用sql,table, コード、Github workflow fileを提案して下さい。
