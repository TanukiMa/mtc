# process_stanza.py
import stanza
from process_common import run_processor
from db_utils import SentenceQueue

def load_stanza_model():
    """Stanzaモデルをロードする"""
    stanza.download('ja', verbose=False) # モデルがなければダウンロード
    return stanza.Pipeline('ja', verbose=False)

def process_batch_with_stanza(sentences, nlp):
    """
    Stanzaを使って文章のバッチを処理する
    """
    # StanzaのDocumentオブジェクトを作成して一括処理
    docs = [stanza.Document([], text=s) for s in sentences]
    processed_docs = nlp.bulk_process(docs)
    # 例: for doc in processed_docs: for sent in doc.sentences: for word in sent.words: print(word.text, word.pos)

def main():
    run_processor(
        processor_name="Stanza",
        model_loader_func=load_stanza_model,
        batch_processor_func=process_batch_with_stanza,
        db_status_column=SentenceQueue.stanza_status
    )

if __name__ == "__main__":
    main()