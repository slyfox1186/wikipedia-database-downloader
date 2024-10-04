# config.py

import os

# ------------------- Global Variable Setup ----------------

class Config:
    ALLOWED_EXTENSIONS = {'html'}
    # --------------------- Log Setup ----------------------
    LOG_FILE = 'download_wikipedia.log'
    LOG_FORMAT = os.environ.get('LOG_FORMAT', '[%(asctime)s] [%(levelname)s] %(message)s')
    LOG_LEVEL = 'INFO   '
    LOG_MAX_BYTES = 10485760  # 10 MB
    # -------------------- Internet Setup--------------------
    CHECKSUM = None
    CHUNK_SIZE = 1024 * 1024
    CONNECTION_COOLDOWN = 5
    MAX_RETRIES = 20
    NUM_CONNECTIONS = 16
    RETRY_BACKOFF = 2.0
    WEB_TIMEOUT = 30
    USER_AGENT = os.environ.get(
        'USER_AGENT_STRING',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0'
    )
    # -------------------- Wikipedia Setup-------------------
    DOWNLOAD_FOLDER = './database_files'
    DOWNLOAD_URL_FULL = 'https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2' # For a list of possible downloads visit: https://dumps.wikimedia.org/enwiki/latest/
    DOWNLOAD_URL_TEST = 'https://dumps.wikimedia.org/simplewiki/latest/simplewiki-latest-stub-articles.xml.gz' # For a list of possible downloads visit: https://dumps.wikimedia.org/simplewiki/latest/
