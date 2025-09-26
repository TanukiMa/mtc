import os

# 基本設定
BASE_URL = "https://www.mhlw.go.jp"
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '3'))
REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 1  # リクエスト間隔（秒）

# ファイルタイプ設定
SUPPORTED_EXTENSIONS = ['.pdf', '.docx', '.pptx', '.html']
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

# 対象URLパターン
TARGET_URL_PATTERNS = [
    "/stf/seisakunitsuite/bunya/kenkou_iryou/",
    "/stf/seisakunitsuite/bunya/koyou_roudou/",
    "/toukei/",
    "/shingi/",
]

# 除外URLパターン  
EXCLUDE_PATTERNS = [
    "/english/",
    "/photo/",
    "/kanren/",
    "javascript:",
    "mailto:",
]

# 形態素解析設定
MIN_WORD_LENGTH = 2
TARGET_POS = ['名詞', '動詞', '形容詞']  # 対象品詞

# LLM設定
LLM_CONTEXT_SIZE = 2048
LLM_THREADS = 4
NEW_WORD_CONFIDENCE_THRESHOLD = 0.5
