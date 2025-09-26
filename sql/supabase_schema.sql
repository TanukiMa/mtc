-- 処理済みURLを管理
CREATE TABLE processed_urls (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    content_type VARCHAR(10) NOT NULL, -- 'html', 'pdf', 'docx', 'pptx'
    last_processed TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    file_hash TEXT, -- コンテンツの変更検知用
    status VARCHAR(20) DEFAULT 'completed', -- 'processing', 'completed', 'failed'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 抽出された単語を保存
CREATE TABLE extracted_words (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    word TEXT NOT NULL,
    reading TEXT, -- 読み方
    part_of_speech TEXT, -- 品詞
    url_id UUID REFERENCES processed_urls(id),
    frequency INTEGER DEFAULT 1,
    first_found TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 新語候補
CREATE TABLE new_word_candidates (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    word TEXT NOT NULL,
    reading TEXT,
    part_of_speech TEXT,
    confidence_score FLOAT, -- LLMによる新語判定スコア
    llm_reasoning TEXT, -- LLMの判定理由
    human_verified BOOLEAN DEFAULT FALSE,
    verification_date TIMESTAMP WITH TIME ZONE,
    source_urls TEXT[], -- 発見元URL
    frequency_count INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 既存辞書（比較用）
CREATE TABLE dictionary_words (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    word TEXT UNIQUE NOT NULL,
    reading TEXT,
    part_of_speech TEXT,
    source VARCHAR(50), -- 'ipadic', 'sudachi', 'custom'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- クローリングログ
CREATE TABLE crawling_logs (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    run_id VARCHAR(100), -- GitHub Actions run ID
    start_time TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    end_time TIMESTAMP WITH TIME ZONE,
    urls_processed INTEGER DEFAULT 0,
    new_words_found INTEGER DEFAULT 0,
    errors_count INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'running' -- 'running', 'completed', 'failed'
);

-- インデックス作成
CREATE INDEX idx_processed_urls_url ON processed_urls(url);
CREATE INDEX idx_processed_urls_hash ON processed_urls(file_hash);
CREATE INDEX idx_extracted_words_word ON extracted_words(word);
CREATE INDEX idx_new_word_candidates_word ON new_word_candidates(word);
CREATE INDEX idx_dictionary_words_word ON dictionary_words(word);

-- RLS (Row Level Security) 設定
ALTER TABLE processed_urls ENABLE ROW LEVEL SECURITY;
ALTER TABLE extracted_words ENABLE ROW LEVEL SECURITY;
ALTER TABLE new_word_candidates ENABLE ROW LEVEL SECURITY;
ALTER TABLE dictionary_words ENABLE ROW LEVEL SECURITY;
ALTER TABLE crawling_logs ENABLE ROW LEVEL SECURITY;
