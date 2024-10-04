    # config.py

import os

# ------------------- Global Variable Setup ----------------

class Config:
    ALLOWED_EXTENSIONS = {'html'}
    # --------------------- Log Setup ----------------------
    DATE_FMT = '%m-%d-%Y_%H:%M:%S-%p'
    LOG_FILE = 'download_wikipedia.log'
    LOG_FORMAT = '%(asctime)s ::: %(levelname)s ::: %(message)s'
    LOG_LEVEL = 'DEBUG'  # Changed to DEBUG to capture detailed logs
    LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
    # -------------------- Internet Setup--------------------
    CHECKSUM = None
    CHUNK_SIZE = 1048576  # 1 MB
    CONNECTION_COOLDOWN = 10  # seconds
    INCREASE_FAILURE_LIMIT = 2
    INCREASE_WAIT_TIME = 900  # seconds
    MAX_CONNECTIONS = 3
    MAX_PART_SIZE_MB = 25  # Ensure this is consistent
    MAX_RETRIES = 20
    MAX_PART_SIZE = 10 * 1024 * 1024  # Number of file parts
    OPTIMAL_CONNECTION_TIMEOUT = 300  # seconds
    MAX_BACKOFF = 2.0  # Backoff factor
    WEB_TIMEOUT = 30  # seconds
    USER_AGENT = os.environ.get(
        'USER_AGENT',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0'
    )
    # -------------------- Wikipedia Setup-------------------
    DOWNLOAD_FOLDER = './database_files'
    # For a list of possible downloads visit either: https://dumps.wikimedia.org/enwiki/latest/ || https://dumps.wikimedia.org/simplewiki/latest/
    # LARGE_DOWNLOAD_URL = 'http://download.wikimedia.org/enwiki/20240920/enwiki-20240920-pages-articles-multistream-index.txt.bz2'
    LARGE_DOWNLOAD_URL = 'https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2'
    SMALL_DOWNLOAD_URL = 'https://dumps.wikimedia.org/simplewiki/latest/simplewiki-latest-stub-articles.xml.gz'
