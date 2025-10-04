# process_ginza.py
import spacy
from process_common import run_processor
from db_utils import SentenceQueue

def load_ginza_model():
    """GiNZAモデルをロードする"""
    return spacy.load("ja_ginza")

def process_batch_with_ginza(sentences, nlp):
    """
    GiNZAを使って文章のバッチを処理する
    （現在は処理するだけだが、将来的に単語抽出などの処理を追加できる）
    """
    # nlp.pipeはジェネレータを返すため、list()で実行を強制
    docs = list(nlp.pipe(sentences))
    # 例: for doc in docs: for token in doc: print(token.text, token.pos_)

def main():
    run_processor(
        processor_name="GiNZA",
        model_loader_func=load_ginza_model,
        batch_processor_func=process_batch_with_ginza,
        db_status_column=SentenceQueue.ginza_status
    )

if __name__ == "__main__":
    main()