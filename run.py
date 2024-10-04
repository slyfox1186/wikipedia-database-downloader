# run.py

# Name: run.py
# GitHub: https://github.com/slyfox1186/wikipedia-database-downloader/blob/main/run.py
# Note: These files are typically over 19 GB compressed and can expand to over 86 GB when decompressed!

import aiofiles
import aiohttp
import argparse
import asyncio
import hashlib
import logging
import math
import os
import random
import shutil
import signal
import sys
import tempfile
import time
import yaml
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from config import Config
from functools import partial
from pathlib import Path
from tqdm.asyncio import tqdm

# --------------------- Custom Exceptions ---------------------

class ServiceUnavailableError(Exception):
    """Exception raised when a 503 Service Unavailable error is encountered."""
    pass

# --------------------- ANSI Color Codes ---------------------

class ANSI:
    RESET = "\u001b[0m"
    BOLD = "\u001b[1m"
    UNDERLINE = "\u001b[4m"

    # Regular Colors
    BLACK = "\u001b[30m"
    RED = "\u001b[31m"
    GREEN = "\u001b[32m"
    YELLOW = "\u001b[33m"
    BLUE = "\u001b[34m"
    MAGENTA = "\u001b[35m"
    CYAN = "\u001b[36m"
    WHITE = "\u001b[37m"

    # Bright Colors
    BRIGHT_BLACK = "\u001b[90m"
    BRIGHT_RED = "\u001b[91m"
    BRIGHT_GREEN = "\u001b[92m"
    BRIGHT_YELLOW = "\u001b[93m"
    BRIGHT_BLUE = "\u001b[94m"
    BRIGHT_MAGENTA = "\u001b[95m"
    BRIGHT_CYAN = "\u001b[96m"
    BRIGHT_WHITE = "\u001b[97m"

# --------------------- Color Formatter ---------------------

class ColorFormatter(logging.Formatter):
    """
    Custom logging formatter to add ANSI color codes to log messages based on severity level.
    """

    LEVEL_COLOR_MAP = {
        logging.DEBUG: ANSI.BRIGHT_BLUE,
        logging.INFO: ANSI.BRIGHT_GREEN,
        logging.WARNING: ANSI.BRIGHT_YELLOW,
        logging.ERROR: ANSI.BRIGHT_RED,
        logging.CRITICAL: ANSI.BRIGHT_MAGENTA,
    }

    DATE_COLOR = ANSI.BRIGHT_CYAN
    SEPARATOR_COLOR = ANSI.WHITE
    MESSAGE_COLOR = ANSI.WHITE

    def format(self, record):
        # Format the original log message
        original = super().format(record)

        # Split the log message into its components
        try:
            timestamp, level, message = original.split(" | ", 2)
        except ValueError:
            # If the log format is unexpected, return the original message without coloring
            return original

        # Apply colors to each component
        timestamp_colored = f"{self.DATE_COLOR}{timestamp}{ANSI.RESET}"
        level_color = self.LEVEL_COLOR_MAP.get(record.levelno, ANSI.WHITE)
        level_colored = f"{level_color}{level}{ANSI.RESET}"
        separator_colored = f"{self.SEPARATOR_COLOR} | {ANSI.RESET}"
        message_colored = f"{self.MESSAGE_COLOR}{message}{ANSI.RESET}"

        # Combine the colored components
        colored_log = f"{timestamp_colored}{separator_colored}{level_colored}{separator_colored}{message_colored}"

        return colored_log

# --------------------- Configuration ---------------------

DEFAULT_CONFIG = {
    'checksum': Config.CHECKSUM,
    'chunk_size': Config.CHUNK_SIZE,
    'connection_cooldown': Config.CONNECTION_COOLDOWN,  # Cooldown period in seconds
    'download_folder': Config.DOWNLOAD_FOLDER,
    'increase_failure_limit': Config.INCREASE_FAILURE_LIMIT,
    'increase_wait_time': Config.INCREASE_WAIT_TIME,  # 15 minutes
    'log_file': Config.LOG_FILE,
    'max_backoff': Config.MAX_BACKOFF,  # Maximum backoff time in seconds
    'max_part_size_mb': Config.MAX_PART_SIZE_MB,
    'max_retries': Config.MAX_RETRIES,  # Maximum number of retries
    'num_connections': Config.MAX_CONNECTIONS,  # Initial number of connections
    'optimal_connection_timeout': Config.OPTIMAL_CONNECTION_TIMEOUT,  # 5 minutes
    'retry_backoff': Config.MAX_BACKOFF,  # Backoff factor
    'timeout': Config.WEB_TIMEOUT,
    'url': Config.LARGE_DOWNLOAD_URL,
    'user_agent': Config.USER_AGENT,
}

# --------------------- Logging Setup ---------------------

logger = logging.getLogger("DownloadWikipedia")
logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture detailed logs

# Updated Formatter with Structured Alignment
formatter = logging.Formatter(Config.LOG_FORMAT, datefmt=Config.DATE_FMT)

# Color Formatter for Console
color_formatter = ColorFormatter(Config.LOG_FORMAT, datefmt=Config.DATE_FMT)

# Console Handler
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)  # Set to DEBUG to display debug logs in console
ch.setFormatter(color_formatter)  # Use color formatter for console
logger.addHandler(ch)

# File Handler
fh = logging.FileHandler(Config.LOG_FILE)
fh.setLevel(logging.DEBUG)  # Set to DEBUG to capture debug logs in file
fh.setFormatter(formatter)  # Use regular formatter for file
logger.addHandler(fh)

# Add Static Headers to Log File
def add_log_headers(log_file: str):
    """
    Adds static headers to the log file if it's newly created or empty.
    """
    if not os.path.exists(log_file) or os.path.getsize(log_file) == 0:
        header = "Timestamp           | Level    | Message\n" \
                 "--------------------|----------|---------------------------------------------------------------\n"
        with open(log_file, 'a') as f:
            f.write(header)

add_log_headers(Config.LOG_FILE)

# --------------------- Argument Parsing ---------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Wikipedia dump downloader based on official guidelines")
    parser.add_argument('--checksum', type=str, help='Expected checksum of the file (e.g., MD5, SHA256).')
    parser.add_argument('--chunk-size', type=int, help='Chunk size for downloading (in bytes).')
    parser.add_argument('--config', type=str, help='Path to YAML configuration file.', default=None)
    parser.add_argument('--connections', type=int, help='Number of concurrent connections.')
    parser.add_argument('--folder', type=str, help='Download folder path.')
    parser.add_argument('--max-part-size-mb', type=int, help='Maximum size per part in megabytes.')
    parser.add_argument('--max-retries', type=int, help='Maximum number of retries for failed downloads.')
    parser.add_argument('--retry-backoff', type=float, help='Backoff factor for retries.')
    parser.add_argument('--timeout', type=int, help='Timeout for HTTP requests in seconds.')
    parser.add_argument('--url', type=str, help='URL of the Wikipedia dump.')
    parser.add_argument('--user-agent', type=str, help='Custom user agent string for the download request.')
    args = parser.parse_args()
    return args

# --------------------- Configuration Loader ---------------------

def load_config(args):
    # Start with DEFAULT_CONFIG
    config = DEFAULT_CONFIG.copy()
    
    # Override with attributes from Config class
    config.update({
        k.lower(): getattr(Config, k) 
        for k in dir(Config) 
        if not k.startswith('__') and not callable(getattr(Config, k))
    })
    
    # Override with command-line arguments if provided
    if args.url:
        config['url'] = args.url
        logger.debug(f"Using URL from command-line argument: {args.url}")
    if args.folder:
        config['download_folder'] = args.folder
        logger.debug(f"Using download folder from command-line argument: {args.folder}")
    if args.connections:
        config['num_connections'] = args.connections
        logger.debug(f"Using number of connections from command-line argument: {args.connections}")
    if args.chunk_size:
        config['chunk_size'] = args.chunk_size
        logger.debug(f"Using chunk size from command-line argument: {args.chunk_size}")
    if args.max_retries:
        config['max_retries'] = args.max_retries
        logger.debug(f"Using max retries from command-line argument: {args.max_retries}")
    if args.retry_backoff:
        config['retry_backoff'] = args.retry_backoff
        logger.debug(f"Using retry backoff from command-line argument: {args.retry_backoff}")
    if args.timeout:
        config['timeout'] = args.timeout
        logger.debug(f"Using timeout from command-line argument: {args.timeout}")
    if args.checksum:
        config['checksum'] = args.checksum
        logger.debug(f"Using checksum from command-line argument: {args.checksum}")
    if args.user_agent:
        config['user_agent'] = args.user_agent
        logger.debug(f"Using user agent from command-line argument: {args.user_agent}")
    if args.max_part_size_mb:
        config['max_part_size_mb'] = args.max_part_size_mb
        logger.debug(f"Using max_part_size_mb from command-line argument: {args.max_part_size_mb}")
    return config

# --------------------- Helper Functions ---------------------

def get_file_name(url):
    return url.split('/')[-1]

def get_temp_file_path(url: str) -> Path:
    """
    Generates a URL-specific temporary file path by hashing the URL.
    """
    hashed_url = hashlib.sha256(url.encode()).hexdigest()
    temp_dir = Path(tempfile.gettempdir()) / "download_wikipedia_connections"
    temp_dir.mkdir(exist_ok=True)
    temp_file = temp_dir / f"{hashed_url}.yaml"
    return temp_file

async def prompt_user(prompt: str) -> bool:
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(None, lambda: input(prompt).strip().lower())
    return response in ['y', 'yes', '']

async def get_ideal_connections(temp_file: Path) -> int:
    if temp_file.exists():
        try:
            async with aiofiles.open(temp_file, 'r') as f:
                content = await f.read()
                data = yaml.safe_load(content)
                ideal_connections = data.get('ideal_connections', None)
                if isinstance(ideal_connections, int) and ideal_connections >=1:
                    return ideal_connections
        except Exception as e:
            logger.warning(f"Failed to read ideal connections from temp file: {e}")
    return None

async def save_ideal_connections(temp_file: Path, connections: int):
    try:
        async with aiofiles.open(temp_file, 'w') as f:
            data = {'ideal_connections': connections}
            await f.write(yaml.dump(data))
        logger.debug(f"Ideal max connections ({connections}) saved to {temp_file}")
    except Exception as e:
        logger.warning(f"Failed to save ideal connections to temp file: {e}")

async def get_user_preference(url: str, default_connections: int) -> int:
    temp_file = get_temp_file_path(url)
    ideal_connections = await get_ideal_connections(temp_file)
    if ideal_connections:
        logger.debug(f"Temporary file storing optimal connections: {temp_file}")
        print()
        prompt = f"Use previously saved ideal max connections ({ideal_connections}) for URL '{url}'? [Y/n]: "
        use_ideal = await prompt_user(prompt)
        if use_ideal:
            logger.debug(f"Using ideal max connections: {ideal_connections}")
            return ideal_connections
    logger.debug(f"Using default number of connections: {default_connections}")
    return default_connections

# --------------------- File Size Retrieval ---------------------

async def get_file_size(session: ClientSession, url: str) -> (int, bool):
    try:
        async with session.head(url) as response:
            if response.status in [200, 206]:
                total_size = int(response.headers.get('Content-Length', 0))
                accept_ranges = response.headers.get('Accept-Ranges', 'none').lower() == 'bytes'
                logger.debug(f"Retrieved file size: {total_size} bytes. Accept-Ranges: {accept_ranges}")
                return total_size, accept_ranges
            else:
                raise Exception(f"Failed to get file size. HTTP status [{response.status}]")
    except Exception as e:
        logger.error(f"Error fetching file size: {e}")
        raise

# --------------------- Connection Manager ---------------------

class ConnectionManager:
    def __init__(self, initial_connections: int, cooldown_period: int, max_backoff: float, min_connections: int = 1):
        self.current_connections = initial_connections
        self.lock = asyncio.Lock()
        self.cooldown_period = cooldown_period  # in seconds
        self.last_reduction_time = None
        self.cooldown_message_sent = False  # Flag to track if cooldown message has been sent
        self.max_backoff = max_backoff
        self.min_connections = min_connections  # Minimum number of connections

    async def decrease_connections(self):
        async with self.lock:
            current_time = time.time()
            if self.last_reduction_time:
                elapsed = current_time - self.last_reduction_time
                if elapsed < self.cooldown_period:
                    remaining = int(self.cooldown_period - elapsed)
                    if not self.cooldown_message_sent:
                        logger.warning(f"Cooldown active. Next connection reduction available in {remaining} seconds")
                        self.cooldown_message_sent = True
                    return  # Do not decrease connections yet

            previous = self.current_connections
            if self.current_connections > 2:
                self.current_connections = max(self.current_connections - 1, 2)
            elif self.current_connections > self.min_connections:
                self.current_connections = max(self.current_connections - 1, self.min_connections)
            # Else, keep it at min_connections

            if self.current_connections < previous:
                self.last_reduction_time = current_time
                self.cooldown_message_sent = False  # Reset the flag
                logger.warning(f"Reducing number of connections to {self.current_connections} and retrying..")
                logger.warning(f"Max connections being tested in the next loop: {self.current_connections}")

    async def can_reduce(self):
        async with self.lock:
            if self.last_reduction_time is None:
                return True
            elapsed = time.time() - self.last_reduction_time
            return elapsed >= self.cooldown_period

    async def get_current_connections(self):
        async with self.lock:
            return self.current_connections

    async def set_connections(self, new_connections: int):
        async with self.lock:
            self.current_connections = new_connections
            self.last_reduction_time = time.time()

# --------------------- Download Range Function ---------------------

# Add this global variable near the top of the file
parts_downloaded = 0

async def download_range(session: ClientSession, url: str, start: int, end: int, part_path: Path, retries: int, backoff: float, progress: tqdm, user_agent: str, conn_manager: ConnectionManager, total_parts: int, total_size: int, max_backoff: float, part_number: int):
    global parts_downloaded
    headers = {
        'Range': f'bytes={start}-{end}',
        'User-Agent': user_agent
    }
    attempt = 0
    while attempt <= retries:
        try:
            async with session.get(url, headers=headers) as response:
                logger.debug(f"Downloading Part [{part_number}] ::: Status [{response.status}]")
                if response.status in [206, 200]:
                    if part_path.exists():
                        mode = 'rb+'
                        logger.debug(f"Opening existing Part file {part_path} in [rb+] mode")
                    else:
                        mode = 'wb'
                        logger.debug(f"Creating new Part file {part_path} in [wb] mode")

                    start_time = time.time()
                    bytes_downloaded = 0
                    async with aiofiles.open(part_path, mode) as f:
                        if mode == 'rb+':
                            await f.seek(start)
                            logger.debug(f"Seeking to byte {start} in Part file {part_path}")
                        async for chunk in response.content.iter_chunked(1024 * 1024):  # 1MB chunks
                            if chunk:
                                await f.write(chunk)
                                bytes_downloaded += len(chunk)
                                progress.update(len(chunk))
                    
                    end_time = time.time()
                    download_time = end_time - start_time
                    if download_time > 0:
                        download_speed = bytes_downloaded / download_time / 1024 / 1024  # in MB/s
                    else:
                        download_speed = 0.0
                    
                    parts_downloaded += 1
                    percentage = int((parts_downloaded / total_parts) * 100)
                    
                    # Calculate zero-padded part number
                    pad_width = len(str(total_parts))
                    part_number_padded = str(part_number).zfill(pad_width)
                    remaining_parts = total_parts - parts_downloaded
                    remaining_padded = str(remaining_parts).zfill(pad_width)

                    # Updated log message with improved formatting
                    logger.debug(f"Successfully Downloaded ::: Part {part_number_padded}/{total_parts} ::: Remaining {remaining_padded} ::: Speed {download_speed:.2f} MB/s")
                    return
                elif response.status == 503:
                    attempt += 1
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        wait = float(retry_after)
                        logger.warning(f"[503 Error] with Retry-After {retry_after} seconds... Retrying in {wait:.2f} seconds... (Attempt {attempt}/{retries})")
                    else:
                        wait = min((backoff ** attempt) + random.uniform(0, 1), max_backoff)
                        logger.warning(f"[503 Error] Service Unavailable. Retrying in {wait:.2f} seconds... (Attempt {attempt}/{retries})")
                    await conn_manager.decrease_connections()
                    await asyncio.sleep(wait)
                else:
                    raise Exception(f"Unexpected status code [{response.status}]")
        except asyncio.CancelledError:
            logger.error(f"Download task was cancelled")
            raise
        except Exception as e:
            attempt += 1
            wait = min((backoff ** attempt) + random.uniform(0, 1), max_backoff)
            logger.warning(f"Error downloading part {part_number}: {type(e).__name__}: {e}. Retrying in {wait:.2f} seconds... (Attempt {attempt}/{retries})")
            await conn_manager.decrease_connections()
            await asyncio.sleep(wait)
    raise ServiceUnavailableError(f"Failed to download part {part_number} after {retries} attempts")

# --------------------- Merge Files Function ---------------------

async def merge_files(part_paths: list, destination: Path, chunk_size: int = 1024 * 1024):
    try:
        logger.info(f"Merging {len(part_paths)} parts into {destination}")
        async with aiofiles.open(destination, 'wb') as outfile:
            for part_path in part_paths:
                logger.debug(f"Merging Part file {part_path} into {destination}")
                async with aiofiles.open(part_path, 'rb') as infile:
                    while True:
                        chunk = await infile.read(chunk_size)
                        if not chunk:
                            break
                        await outfile.write(chunk)
                        logger.debug(f"Wrote {len(chunk)} bytes from {part_path} to {destination}")
        logger.info("All parts merged successfully")
    except Exception as e:
        logger.error(f"Error merging files: {e}")
        raise

# --------------------- Verify Checksum Function ---------------------

async def verify_checksum(file_path: Path, expected_checksum: str, algorithm: str = 'md5'):
    try:
        hash_func = getattr(hashlib, algorithm)()
    except AttributeError:
        raise ValueError(f"Unsupported checksum algorithm: {algorithm}")
    
    try:
        logger.info(f"Starting checksum verification using {algorithm.upper()} for {file_path}")
        async with aiofiles.open(file_path, 'rb') as f:
            while True:
                chunk = await f.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break
                hash_func.update(chunk)
        calculated = hash_func.hexdigest()
        if calculated.lower() == expected_checksum.lower():
            logger.info(f"Checksum verification passed ({algorithm.upper()}: {calculated})")
        else:
            raise Exception(f"Checksum verification failed. Expected: {expected_checksum}, Got: {calculated}")
    except Exception as e:
        logger.error(f"Error during checksum verification: {e}")
        raise

# --------------------- Monitor Function ---------------------

async def download_success_monitor(conn_manager: ConnectionManager, temp_file: Path, config: dict):
    """
    Monitors the download process to determine when no [503 Error]s have occurred for a specified duration.
    Once the condition is met, saves the current connection count as the optimal max connections.
    Then, attempts to increase the max connections by 1 until a [503 Error] occurs.
    If two consecutive increases fail, waits for a specified time before trying again.
    """
    duration = config.get('optimal_connection_timeout', 300)  # 5 minutes in seconds
    increase_failure_limit = config.get('increase_failure_limit', 2)
    increase_wait_time = config.get('increase_wait_time', 900)  # 15 minutes in seconds

    logger.debug("Starting download success monitor")

    # Phase 1: Wait for initial stability
    logger.debug("Phase 1: Waiting for initial stability")
    while True:
        await asyncio.sleep(10)  # Check every 10 seconds
        async with conn_manager.lock:
            last_reduction = conn_manager.last_reduction_time
            if last_reduction is None:
                logger.debug("No reductions have occurred yet")
                continue  # No reductions yet
            elapsed = time.time() - last_reduction
            if elapsed >= duration:
                # No reductions in the last 'duration' seconds
                await save_ideal_connections(temp_file, conn_manager.current_connections)
                logger.debug(f"No [503 Error]s detected for {duration / 60} minutes. Ideal max connections ({conn_manager.current_connections}) saved")
                break  # Exit Phase 1
            else:
                remaining = duration - elapsed
                logger.debug(f"Phase 1: {elapsed:.1f} seconds since last reduction. {remaining:.1f} seconds remaining")

    # Phase 2: Attempt to optimize connections
    logger.debug("Phase 2: Starting connection optimization")
    failure_count = 0

    while True:
        await asyncio.sleep(5)  # Small delay before attempting to increase
        async with conn_manager.lock:
            current_connections = conn_manager.current_connections
            new_connections = current_connections + 1
            logger.debug(f"Attempting to increase connections from {current_connections} to {new_connections}")

            # Update the connection count temporarily
            conn_manager.current_connections = new_connections

        # Save the increased connection count
        await save_ideal_connections(temp_file, new_connections)
        logger.debug(f"Max connections increased to {new_connections}")

        # Allow some time for the download to stabilize
        stabilization_time = 60  # 1 minute to observe
        logger.debug(f"Stabilization time: {stabilization_time} seconds")
        await asyncio.sleep(stabilization_time)

        async with conn_manager.lock:
            # Check if any reductions have occurred during stabilization
            last_reduction = conn_manager.last_reduction_time
            if last_reduction and (time.time() - last_reduction) < conn_manager.cooldown_period:
                # A reduction occurred, meaning the increase caused issues
                logger.debug(f"Increasing connections to {new_connections} caused a [503 Error]. Decreasing back to {current_connections}")
                conn_manager.current_connections = current_connections
                failure_count += 1
                await save_ideal_connections(temp_file, current_connections)
                logger.debug(f"Max connections reverted to {current_connections} due to failure")
                if failure_count >= increase_failure_limit:
                    logger.warning(f"Reached failure limit of {increase_failure_limit}. Waiting for {increase_wait_time / 60} minutes before retrying")
                    await asyncio.sleep(increase_wait_time)
                    failure_count = 0  # Reset failure count after waiting
            else:
                # No reductions occurred, the increase was successful
                logger.debug(f"Increase to {new_connections} connections was successful")
                failure_count = 0  # Reset failure count on success

# --------------------- Main Download Function ---------------------

async def download_file(config):
    url = config['url']
    download_folder = Path(config['download_folder'])
    download_folder.mkdir(parents=True, exist_ok=True)
    file_name = get_file_name(url)
    destination = download_folder / file_name
    temp_dir = download_folder / f".{file_name}.parts"
    temp_dir.mkdir(exist_ok=True)

    logger.warning("Please ensure you have sufficient storage space. The compressed file is typically over 19 GB and expands to over 86 GB when decompressed")
    logger.warning("Also check your system's file size limits, especially on 32-bit systems")

    default_connections = config['num_connections']
    starting_connections = await get_user_preference(url, default_connections)

    conn_manager = ConnectionManager(
        initial_connections=starting_connections,
        cooldown_period=config.get('connection_cooldown', 30),
        max_backoff=config.get('max_backoff', 60),
        min_connections=1
    )
    num_connections = await conn_manager.get_current_connections()

    # Start the monitor task
    temp_file = get_temp_file_path(url)
    monitor_task = asyncio.create_task(download_success_monitor(conn_manager, temp_file, config))

    # Define a cleanup task to cancel monitor_task if download completes or fails
    async def cleanup():
        if not monitor_task.done():
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                logger.debug("Monitor task cancelled")

    try:
        while num_connections >= conn_manager.min_connections:
            logger.debug(f"Attempting download with {num_connections} connection(s)")
            connector = TCPConnector(limit=num_connections, force_close=True)
            timeout = ClientTimeout(total=config['timeout'])
            async with ClientSession(connector=connector, timeout=timeout) as session:
                logger.debug(f"Fetching file information for {url}..")
                try:
                    total_size, accept_ranges = await get_file_size(session, url)
                except Exception as e:
                    logger.error(f"Unable to retrieve file information: {e}")
                    await cleanup()
                    sys.exit(1)

                if total_size == 0:
                    logger.error("File size is 0. Exiting")
                    await cleanup()
                    sys.exit(1)

                logger.debug(f"File size: {total_size / (1024**3):.2f} GB")
                if not accept_ranges:
                    logger.warning("Server does not support byte ranges. Falling back to single connection")
                    await conn_manager.decrease_connections()
                    num_connections = await conn_manager.get_current_connections()
                    continue

                # Determine part division based on max_part_size_mb
                max_part_size_mb = config.get('max_part_size_mb', None)

                if max_part_size_mb:
                    part_size = max_part_size_mb * 1024 * 1024  # Convert MB to bytes
                    calculated_parts = math.ceil(total_size / part_size)
                    part_size = math.ceil(total_size / calculated_parts)  # Recalculate to cover the entire file
                    logger.debug(f"Using {calculated_parts} part(s) with part size {part_size / (1024 * 1024):.2f} MB each based on max_part_size_mb")
                    part_paths = [temp_dir / f"part_{i+1}" for i in range(calculated_parts)]
                    
                    # Log each part's byte range with improved formatting
                    for i in range(calculated_parts):
                        start = part_size * i
                        end = min(start + part_size - 1, total_size - 1)
                        logger.debug(f"Part {i+1}: bytes {start}-{end} ({(end - start + 1) / (1024 * 1024):.2f} MB)")
                else:
                    logger.error("max_part_size_mb is not set. Please set it to proceed with the download")
                    await cleanup()
                    sys.exit(1)

                # Determine ranges and check for existing parts
                ranges = []
                initial_progress = 0
                parts_found = 0
                parts_to_download = 0

                for i in range(calculated_parts):
                    start = part_size * i
                    end = min(start + part_size - 1, total_size - 1)
                    part_path = part_paths[i]
                    
                    if part_path.exists():
                        existing_size = part_path.stat().st_size
                        expected_size = end - start + 1
                        if existing_size == expected_size:
                            parts_found += 1
                            initial_progress += existing_size
                            ranges.append(None)  # Indicate that this part is complete
                            logger.debug(f"Part {i+1} already exists and is complete")
                        elif existing_size < expected_size:
                            parts_to_download += 1
                            resume_start = start + existing_size
                            ranges.append((resume_start, end, i+1))
                            initial_progress += existing_size
                            logger.debug(f"Resuming download for part {i+1} from byte {resume_start}")
                        else:
                            parts_to_download += 1
                            logger.debug(f"Part {i+1} has unexpected size ({existing_size} bytes). Re-downloading")
                            part_path.unlink()  # Remove corrupted part
                            ranges.append((start, end, i+1))
                    else:
                        parts_to_download += 1
                        ranges.append((start, end, i+1))
                        logger.debug(f"Starting download for part {i+1}")

                logger.debug(f"Found {parts_found} complete part(s). {parts_to_download} part(s) left to download")

                if parts_to_download == 0:
                    logger.debug("All parts are already downloaded. Proceeding to merge")
                else:
                    # Create a progress tracker without visual output
                    progress_tracker = tqdm(total=total_size, unit='B', unit_scale=True, desc=file_name, initial=initial_progress, disable=True)

                    # Create download tasks
                    tasks = []
                    global parts_downloaded
                    parts_downloaded = 0  # Reset the counter before starting new downloads
                    max_backoff = config.get('max_backoff', 60)
                    for i, range_info in enumerate(ranges):
                        if range_info is not None:
                            start, end, part_number = range_info
                            part_path = part_paths[i]
                            task = download_range(
                                session=session,
                                url=url,
                                start=start,
                                end=end,
                                part_path=part_path,
                                retries=config['max_retries'],
                                backoff=config['retry_backoff'],
                                progress=progress_tracker,
                                user_agent=config['user_agent'],
                                conn_manager=conn_manager,
                                total_parts=parts_to_download,
                                total_size=total_size,
                                max_backoff=max_backoff,
                                part_number=part_number
                            )
                            tasks.append(task)

                    logger.debug(f"Starting download of {len(tasks)} part(s) with {num_connections} connection(s)")

                    # Execute download tasks with rate limiting
                    semaphore = asyncio.Semaphore(num_connections)

                    async def bounded_download(task):
                        async with semaphore:
                            await task

                    bounded_tasks = [bounded_download(task) for task in tasks]

                    try:
                        await asyncio.gather(*bounded_tasks)
                    except asyncio.CancelledError:
                        logger.error("Download was cancelled")
                        await cleanup()
                        raise
                    except ServiceUnavailableError as e:
                        logger.error(f"Service unavailable: {e}")
                        await conn_manager.decrease_connections()
                        num_connections = await conn_manager.get_current_connections()
                        if num_connections < conn_manager.min_connections:
                            logger.error("Minimum number of connections reached. Exiting")
                            await cleanup()
                            sys.exit(1)
                        else:
                            logger.debug(f"Retrying download with {num_connections} connection(s)")
                            continue  # Retry the loop with fewer connections
                    finally:
                        progress_tracker.close()

                # Merge parts if necessary
                if parts_to_download > 0:
                    logger.debug("Merging parts..")
                    try:
                        await merge_files(part_paths, destination)
                    except Exception as e:
                        logger.error(f"Failed to merge parts: {e}")
                        await cleanup()
                        sys.exit(1)

                    # Remove temporary parts directory
                    try:
                        shutil.rmtree(temp_dir)
                        logger.debug(f"Removed temporary directory {temp_dir}")
                    except Exception as e:
                        logger.warning(f"Could not remove temporary directory {temp_dir}: {e}")

                # Verify checksum if provided
                if config['checksum']:
                    logger.debug("Verifying checksum..")
                    checksum = config['checksum']
                    if ':' in checksum:
                        algorithm, expected = checksum.split(':', 1)
                    else:
                        algorithm, expected = 'md5', checksum
                    try:
                        await verify_checksum(destination, expected, algorithm)
                    except Exception as e:
                        logger.error(f"Checksum verification failed: {e}")
                        await cleanup()
                        sys.exit(1)

                logger.info(f"Download completed successfully: {destination}")
                await cleanup()
                break  # Exit the loop upon successful download

    except asyncio.CancelledError:
        logger.error("Download tasks were cancelled")
        await cleanup()
        sys.exit(1)
    except ServiceUnavailableError as e:
        logger.error(f"Service unavailable error: {e}")
        await cleanup()
        sys.exit(1)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        await cleanup()
        sys.exit(1)

# --------------------- Signal Handler ---------------------

def setup_signal_handlers(loop):
    """
    Sets up signal handlers for graceful shutdown.
    """
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(handle_signal(s)))
            logger.debug(f"Signal handler added for {sig.name}")
        except NotImplementedError:
            logger.warning(f"Signal handling for {sig.name} is not supported on this platform")

async def handle_signal(signal_received):
    """
    Handles received signals by logging and cancelling all tasks.
    """
    logger.info(f"Received exit signal {signal_received.name}... Initiating graceful shutdown")
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("All tasks have been cancelled")
    sys.exit(0)

# --------------------- Entry Point ---------------------

async def async_main():
    args = parse_args()
    config = load_config(args)

    logger.debug(f"Final configuration: {config}")

    loop = asyncio.get_running_loop()
    setup_signal_handlers(loop)

    await download_file(config)

def main():
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
        sys.exit(1)
    except SystemExit:
        pass
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
