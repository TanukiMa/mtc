# process_stanza.py (修正後の完全なコード)
import stanza
from process_common import run_processor
from db_utils import SentenceQueue

def load_stanza_model():
    """Stanzaモデルをロードする"""
    stanza.download('ja', verbose=False)
    return stanza.Pipeline('ja', verbose=False, processors='tokenize,pos,lemma,ner')

def process_batch_with_stanza(sentences, nlp):
    """
    Stanzaを使って文章のバッチを処理し、固有名詞のリストを返す
    """
    discovered_words = []
    
    # StanzaのDocumentオブジェクトを作成して一括処理
    docs = [stanza.Document([], text=s) for s in sentences]
    processed_docs = nlp.bulk_process(docs)
    
    for doc in processed_docs:
        for sent in doc.sentences:
            for word in sent.words:
                # 品詞(pos)が固有名詞(PROPN)のものを抽出
                if word.pos == 'PROPN':
                    discovered_words.append({
                        "word": word.lemma,  # 見出し語（基本形）
                        "source_tool": "stanza",
                        "pos_tag": word.pos,
                        "entity_category": word.parent.ner # 固有表現カテゴリ (e.g., PERSON)
                    })
    return discovered_words

def main():
    run_processor(
        processor_name="Stanza",
        model_loader_func=load_stanza_model,
        batch_processor_func=process_batch_with_stanza,
        db_status_column=SentenceQueue.stanza_status
    )

if __name__ == "__main__":
    main()