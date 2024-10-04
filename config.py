# config.py

import os

# ------------------- Global Variable Setup ----------------

class Config:
    ALLOWED_EXTENSIONS = {'html'}
    # --------------------- Log Setup ----------------------
    LOG_FILE = 'download_wikipedia.log'
    LOG_FORMAT = os.environ.get('LOG_FORMAT', '[%(asctime)s] [%(levelname)s] %(message)s')
    LOG_LEVEL = 'INFO'
    LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
    # -------------------- Internet Setup--------------------
    MAX_TOTAL_PARTS = 100  # Number of file parts
    MAX_AVERAGE_PARTS = 10  # Maximum size per file part in megabytes (set to None by default and is the default)
    CHECKSUM = None
    CHUNK_SIZE = 1024 * 1024  # 1 MB
    CONNECTION_COOLDOWN = 10  # seconds
    MAX_RETRIES = 20
    MAX_CONNECTIONS = 16
    OPTIMAL_CONNECTION_TIMEOUT = 300  # seconds
    INCREASE_FAILURE_LIMIT = 2
    INCREASE_WAIT_TIME = 900  # seconds
    RETRY_BACKOFF = 2.0
    WEB_TIMEOUT = 30  # seconds
    USER_AGENT = os.environ.get(
        'USER_AGENT_STRING',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0'
    )
    # -------------------- Wikipedia Setup-------------------
    DOWNLOAD_FOLDER = './database_files'
    # For a list of possible downloads visit either: https://dumps.wikimedia.org/enwiki/latest/ || https://dumps.wikimedia.org/simplewiki/latest/
    DOWNLOAD_URL_FULL = 'https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2'
    DOWNLOAD_URL_TEST = 'https://dumps.wikimedia.org/simplewiki/latest/simplewiki-latest-stub-articles.xml.gz'
