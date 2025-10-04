# process_ginza.py (修正後の完全なコード)
import spacy
from process_common import run_processor
from db_utils import SentenceQueue

def load_ginza_model():
    """GiNZAモデルをロードする"""
    return spacy.load("ja_ginza_electra")

def process_batch_with_ginza(sentences, nlp):
    """
    GiNZAを使って文章のバッチを処理し、未知語のリストを返す
    """
    discovered_words = []
    docs = nlp.pipe(sentences)
    
    for doc in docs:
        for token in doc:
            # is_oovフラグがTrueの単語（未知語）のみを抽出
            if token.is_oov:
                discovered_words.append({
                    "word": token.lemma_,  # 見出し語（基本形）
                    "source_tool": "ginza",
                    "pos_tag": token.pos_ # 品詞タグ
                })
    return discovered_words

def main():
    run_processor(
        processor_name="GiNZA",
        model_loader_func=load_ginza_model,
        batch_processor_func=process_batch_with_ginza,
        db_status_column=SentenceQueue.ginza_status
    )

if __name__ == "__main__":
    main()