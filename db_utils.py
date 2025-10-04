# db_utils.py
import os
from sqlalchemy import create_engine, text, Column, BigInteger, String, Enum, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from supabase import create_client, Client
import enum

# --- SQLAlchemy Setup ---
Base = declarative_base()

class ProcessStatus(enum.Enum):
    queued = 'queued'
    processing = 'processing'
    completed = 'completed'
    failed = 'failed'

class CrawlQueue(Base):
    __tablename__ = 'crawl_queue'
    id = Column(BigInteger, primary_key=True)
    url = Column(String, unique=True, nullable=False)
    extraction_status = Column(Enum(ProcessStatus), nullable=False, default=ProcessStatus.queued)
    content_hash = Column(String)
    last_modified = Column(String)
    etag = Column(String)
    processed_at = Column(DateTime)

class SentenceQueue(Base):
    __tablename__ = 'sentence_queue'
    id = Column(BigInteger, primary_key=True)
    crawl_queue_id = Column(BigInteger, ForeignKey('crawl_queue.id', ondelete='CASCADE'), nullable=False)
    sentence_text = Column(String, nullable=False)
    ginza_status = Column(Enum(ProcessStatus), nullable=False, default=ProcessStatus.queued)
    stanza_status = Column(Enum(ProcessStatus), nullable=False, default=ProcessStatus.queued)
    crawl_queue = relationship("CrawlQueue")

class UniqueWord(Base):
    __tablename__ = 'unique_words'
    id = Column(BigInteger, primary_key=True)
    word = Column(String, nullable=False)
    source_tool = Column(String, nullable=False)
    entity_category = Column(String)
    pos_tag = Column(String)
    __table_args__ = (UniqueConstraint('word', 'source_tool', name='unique_word_per_tool'),)

class WordOccurrence(Base):
    __tablename__ = 'word_occurrences'
    id = Column(BigInteger, primary_key=True)
    word_id = Column(BigInteger, ForeignKey('unique_words.id', ondelete='CASCADE'), nullable=False)
    source_url = Column(String, nullable=False)
    __table_args__ = (UniqueConstraint('word_id', 'source_url', name='unique_occurrence'),)

class StopWord(Base):
    __tablename__ = 'stop_words'
    id = Column(BigInteger, primary_key=True)
    word = Column(String, unique=True, nullable=False)
    reason = Column(String)

class BoilerplatePattern(Base):
    __tablename__ = 'boilerplate_patterns'
    id = Column(BigInteger, primary_key=True)
    pattern = Column(text, nullable=False, unique=True)
    reason = Column(text)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())

def get_local_db_session():
    """Returns a new session to the local PostgreSQL database."""
    engine = create_engine(os.environ["LOCAL_DB_URL"])
    Session = sessionmaker(bind=engine)
    return Session()

def get_supabase_client() -> Client:
    """Initializes and returns the Supabase client."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("Supabase credentials not provided.")
    return create_client(url, key)
