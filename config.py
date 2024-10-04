# config.py

import os

# ------------------- Global Variable Setup ----------------

class Config:
    ALLOWED_EXTENSIONS = {'html'}
    # --------------------- Log Setup ----------------------
    LOG_FILE = 'download_wikipedia.log'
    LOG_FORMAT = os.environ.get('LOG_FORMAT', '[%(asctime)s] [%(levelname)s] %(message)s')
    LOG_LEVEL = 'INFO' # Set to 'DEBUG' for more detailed logs
    LOG_MAX_BYTES = 10 * 1024 * 1024 # 10 MB
    # -------------------- Internet Setup--------------------
    CHECKSUM = None
    CHUNK_SIZE = 1024 * 1024 # 1 MB
    CONNECTION_COOLDOWN = 10 # seconds
    INCREASE_FAILURE_LIMIT = 2
    INCREASE_WAIT_TIME = 900 # seconds
    MAX_PART_SIZE = 10 # Maximum size per file part in megabytes
    MAX_CONNECTIONS = 3
    MAX_RETRIES = 10 # Reduced from 200 to 10
    OPTIMAL_CONNECTION_TIMEOUT = 300 # seconds
    RETRY_BACKOFF = 2.0
    WEB_TIMEOUT = 15 # seconds
    USER_AGENT = os.environ.get(
        'USER_AGENT',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0'
    )
    MAX_BACKOFF = 10 # seconds
    # -------------------- Wikipedia Setup-------------------
    DOWNLOAD_FOLDER = './database_files'
    # DOWNLOAD_URL_TEST = 'http://download.wikimedia.org/enwiki/20240920/enwiki-20240920-pages-articles-multistream-index.txt.bz2'
    DOWNLOAD_URL_FULL = 'https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2' # Large model for production
    DOWNLOAD_URL_TEST = 'https://dumps.wikimedia.org/simplewiki/latest/simplewiki-latest-stub-articles.xml.gz' # Smaller model great for testing

# For a list of possible downloads visit either site:
# https://dumps.wikimedia.org/enwiki/latest/
# https://dumps.wikimedia.org/simplewiki/latest/
