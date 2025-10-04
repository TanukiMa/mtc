# db_utils.py
import os
import configparser
from sqlalchemy import create_engine, Column, BigInteger, Text, TIMESTAMP, Enum
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from supabase import create_client, Client

# --- Supabase Client ---
_supabase_client = None

def get_supabase_client() -> Client:
    """
    Returns a singleton instance of the Supabase client.
    """
    global _supabase_client
    if _supabase_client is None:
        config = configparser.ConfigParser()
        config.read('config.ini')
        url = config['Supabase']['URL']
        key = config['Supabase']['ANON_KEY']
        _supabase_client = create_client(url, key)
    return _supabase_client

# --- SQLAlchemy Local DB Setup ---
Base = declarative_base()
_local_session_factory = None

def get_local_db_session():
    """
    Returns a session for the local PostgreSQL database.
    """
    global _local_session_factory
    if _local_session_factory is None:
        db_url = os.environ.get("LOCAL_DB_URL")
        if not db_url:
            raise ValueError("Environment variable LOCAL_DB_URL is not set.")
        engine = create_engine(db_url)
        _local_session_factory = sessionmaker(bind=engine)
    return _local_session_factory()

# --- ORM Model Classes ---
ProcessStatusEnum = Enum('queued', 'processing', 'completed', 'failed', name='process_status_enum')

class CrawlQueue(Base):
    __tablename__ = 'crawl_queue'
    id = Column(BigInteger, primary_key=True)
    url = Column(Text, nullable=False, unique=True)
    extraction_status = Column(ProcessStatusEnum, nullable=False, default='queued')
    content_hash = Column(Text)
    last_modified = Column(Text)
    etag = Column(Text)
    processed_at = Column(TIMESTAMP(timezone=True))

class SentenceQueue(Base):
    __tablename__ = 'sentence_queue'
    id = Column(BigInteger, primary_key=True)
    crawl_queue_id = Column(BigInteger, nullable=False)
    sentence_text = Column(Text, nullable=False)
    ginza_status = Column(ProcessStatusEnum, nullable=False, default='queued')
    stanza_status = Column(ProcessStatusEnum, nullable=False, default='queued')

class StopWord(Base):
    __tablename__ = 'stop_words'
    id = Column(BigInteger, primary_key=True)
    word = Column(Text, nullable=False, unique=True)
    reason = Column(Text)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())

class BoilerplatePattern(Base):
    """
    Model for storing boilerplate patterns to be excluded.
    """
    __tablename__ = 'boilerplate_patterns'
    id = Column(BigInteger, primary_key=True)
    # --- FIX: Changed 'text' to 'Text' ---
    pattern = Column(Text, nullable=False, unique=True)
    reason = Column(Text)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())