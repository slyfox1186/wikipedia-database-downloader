# download_wikipedia.py

#!/usr/bin/env python3

import aiofiles
import aiohttp
import argparse
import asyncio
import hashlib
import logging
import math
import os
import signal
import sys
import tempfile
import yaml
import random
import shutil
import time
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from functools import partial
from pathlib import Path
from tqdm.asyncio import tqdm
from config import Config

# --------------------- Custom Exceptions ---------------------

class ServiceUnavailableError(Exception):
    """Exception raised when a 503 Service Unavailable error is encountered."""
    pass

# --------------------- Connection Manager ---------------------

class ConnectionManager:
    def __init__(self, initial_connections: int, cooldown_period: int):
        self.current_connections = initial_connections
        self.lock = asyncio.Lock()
        self.cooldown_period = cooldown_period  # in seconds
        self.last_reduction_time = None

    async def decrease_connections(self):
        async with self.lock:
            current_time = time.time()
            if self.last_reduction_time:
                elapsed = current_time - self.last_reduction_time
                if elapsed < self.cooldown_period:
                    remaining = self.cooldown_period - elapsed
                    logger.info(f"Cooldown active. Next connection reduction available in {remaining:.1f} seconds.")
                    return  # Do not decrease connections yet

            previous = self.current_connections
            if self.current_connections > 4:
                self.current_connections = max(self.current_connections - 2, 4)
            elif self.current_connections > 1:
                self.current_connections = max(self.current_connections - 1, 1)
            # Else, keep it at 1

            if self.current_connections < previous:
                self.last_reduction_time = current_time
                logger.warning(f"Reducing number of connections to {self.current_connections} and retrying...")
                logger.info(f"Max connections being tested in the next loop: {self.current_connections}")

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

# --------------------- Configuration ---------------------

DEFAULT_CONFIG = {
    'url': Config.DOWNLOAD_URL_FULL,
    'download_folder': Config.DOWNLOAD_FOLDER,
    'num_connections': Config.NUM_CONNECTIONS,
    'chunk_size': Config.CHUNK_SIZE,
    'max_retries': Config.MAX_RETRIES,
    'retry_backoff': Config.RETRY_BACKOFF,
    'timeout': Config.WEB_TIMEOUT,
    'checksum': Config.CHECKSUM,
    'log_file': Config.LOG_FILE,
    'user_agent': Config.USER_AGENT,  # Added user_agent to DEFAULT_CONFIG
    'connection_cooldown': Config.CONNECTION_COOLDOWN,  # Cooldown period in seconds
}

# Note: This file is typically over 19 GB compressed and expands to over 86 GB when decompressed.

# --------------------- Logging Setup ---------------------

logger = logging.getLogger("DownloadWikipedia")
logger.setLevel(logging.getLevelName(Config.LOG_LEVEL))
formatter = logging.Formatter(Config.LOG_FORMAT)

# Console Handler
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.getLevelName(Config.LOG_LEVEL))
ch.setFormatter(formatter)
logger.addHandler(ch)

# File Handler
fh = logging.FileHandler(Config.LOG_FILE)
fh.setLevel(logging.getLevelName(Config.LOG_LEVEL))
fh.setFormatter(formatter)
logger.addHandler(fh)

# --------------------- Argument Parsing ---------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Wikipedia dump downloader based on official guidelines.")
    parser.add_argument('--config', type=str, help='Path to YAML configuration file.', default=None)
    parser.add_argument('--url', type=str, help='URL of the Wikipedia dump.')
    parser.add_argument('--folder', type=str, help='Download folder path.')
    parser.add_argument('--connections', type=int, help='Number of concurrent connections.')
    parser.add_argument('--chunk-size', type=int, help='Chunk size for downloading (in bytes).')
    parser.add_argument('--max-retries', type=int, help='Maximum number of retries for failed downloads.')
    parser.add_argument('--retry-backoff', type=float, help='Backoff factor for retries.')
    parser.add_argument('--timeout', type=int, help='Timeout for HTTP requests in seconds.')
    parser.add_argument('--checksum', type=str, help='Expected checksum of the file (e.g., MD5, SHA256).')
    parser.add_argument('--user-agent', type=str, help='Custom user agent string for the download request.')
    args = parser.parse_args()
    return args

# --------------------- Configuration Loader ---------------------

def load_config(args):
    config = DEFAULT_CONFIG.copy()
    if args.config:
        try:
            with open(args.config, 'r') as f:
                user_config = yaml.safe_load(f)
            if user_config:
                config.update(user_config)
                logger.info(f"Configuration loaded from {args.config}.")
                logger.debug(f"User configuration: {user_config}")
            else:
                logger.info(f"Configuration file {args.config} is empty.")
        except Exception as e:
            logger.error(f"Failed to load configuration file {args.config}: {e}")
            sys.exit(1)
    
    # Override with command-line arguments if provided
    if args.url:
        config['url'] = args.url
        logger.debug(f"Overriding URL with command-line argument: {args.url}")
    if args.folder:
        config['download_folder'] = args.folder
        logger.debug(f"Overriding download folder with command-line argument: {args.folder}")
    if args.connections:
        config['num_connections'] = args.connections
        logger.debug(f"Overriding number of connections with command-line argument: {args.connections}")
    if args.chunk_size:
        config['chunk_size'] = args.chunk_size
        logger.debug(f"Overriding chunk size with command-line argument: {args.chunk_size}")
    if args.max_retries:
        config['max_retries'] = args.max_retries
        logger.debug(f"Overriding max retries with command-line argument: {args.max_retries}")
    if args.retry_backoff:
        config['retry_backoff'] = args.retry_backoff
        logger.debug(f"Overriding retry backoff with command-line argument: {args.retry_backoff}")
    if args.timeout:
        config['timeout'] = args.timeout
        logger.debug(f"Overriding timeout with command-line argument: {args.timeout}")
    if args.checksum:
        config['checksum'] = args.checksum
        logger.debug(f"Overriding checksum with command-line argument: {args.checksum}")
    if args.user_agent:
        config['user_agent'] = args.user_agent
        logger.debug(f"Overriding user agent with command-line argument: {args.user_agent}")
    return config

# --------------------- Helper Functions ---------------------

def get_file_name(url):
    return url.split('/')[-1]

def get_temp_file_path():
    temp_dir = Path(tempfile.gettempdir())
    temp_file = temp_dir / "download_wikipedia_ideal_connections.yaml"
    return temp_file

async def prompt_user(prompt: str) -> bool:
    loop = asyncio.get_event_loop()
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
        logger.info(f"Ideal max connections ({connections}) saved to {temp_file}.")
    except Exception as e:
        logger.error(f"Failed to save ideal connections to temp file: {e}")

async def verify_no_errors_for_duration(start_time: float, duration: int) -> bool:
    # Placeholder for future implementation
    return False

async def get_user_preference(temp_file: Path, default_connections: int) -> int:
    ideal_connections = await get_ideal_connections(temp_file)
    if ideal_connections:
        prompt = f"Use previously saved ideal max connections ({ideal_connections})? [Y/n]: "
        use_ideal = await prompt_user(prompt)
        if use_ideal:
            logger.info(f"Using ideal max connections: {ideal_connections}")
            return ideal_connections
    logger.info(f"Using default number of connections: {default_connections}")
    return default_connections

# --------------------- File Size Retrieval ---------------------

async def get_file_size(session: ClientSession, url: str) -> (int, bool):
    try:
        async with session.head(url) as response:
            if response.status in [200, 206]:
                total_size = int(response.headers.get('Content-Length', 0))
                accept_ranges = response.headers.get('Accept-Ranges', 'none').lower() == 'bytes'
                return total_size, accept_ranges
            else:
                raise Exception(f"Failed to get file size. HTTP status: {response.status}")
    except Exception as e:
        logger.error(f"Error fetching file size: {e}")
        raise

# --------------------- Helper Functions Continued ---------------------

async def download_range(session: ClientSession, url: str, start: int, end: int, part_path: Path, retries: int, backoff: float, progress: tqdm, user_agent: str, conn_manager: ConnectionManager):
    headers = {
        'Range': f'bytes={start}-{end}',
        'User-Agent': user_agent  # Use user_agent from config
    }
    attempt = 0
    while attempt <= retries:
        try:
            async with session.get(url, headers=headers) as response:
                logger.debug(f"Downloading range {start}-{end}. Status: {response.status}")
                if response.status in [206, 200]:
                    # Determine the correct file mode
                    if part_path.exists():
                        mode = 'rb+'
                        logger.debug(f"Opening existing part file {part_path} in 'rb+' mode.")
                    else:
                        mode = 'wb'
                        logger.debug(f"Creating new part file {part_path} in 'wb' mode.")

                    async with aiofiles.open(part_path, mode) as f:
                        if mode == 'rb+':
                            await f.seek(start)
                            logger.debug(f"Seeking to byte {start} in part file {part_path}.")
                        async for chunk in response.content.iter_chunked(1024 * 1024):  # 1MB chunks
                            if chunk:
                                await f.write(chunk)
                                progress.update(len(chunk))
                                logger.debug(f"Wrote {len(chunk)} bytes to {part_path}.")
                    logger.info(f"Successfully downloaded range {start}-{end}.")
                    return
                elif response.status == 503:
                    attempt += 1
                    wait = (backoff ** attempt) + random.uniform(0, 1)  # Add jitter
                    logger.warning(f"503 Error - Service Unavailable for range {start}-{end}. Retrying in {wait:.2f} seconds... (Attempt {attempt}/{retries})")
                    await conn_manager.decrease_connections()
                    await asyncio.sleep(wait)
                else:
                    raise Exception(f"Unexpected status code {response.status} for range {start}-{end}")
        except asyncio.CancelledError:
            logger.warning(f"Download task for range {start}-{end} was cancelled.")
            raise
        except Exception as e:
            attempt += 1
            wait = (backoff ** attempt) + random.uniform(0, 1)  # Add jitter
            logger.warning(f"Error downloading range {start}-{end}: {e}. Retrying in {wait:.2f} seconds... (Attempt {attempt}/{retries})")
            await asyncio.sleep(wait)
    raise ServiceUnavailableError(f"Failed to download range {start}-{end} after {retries} attempts.")

async def merge_files(part_paths: list, destination: Path, chunk_size: int = 1024 * 1024):
    try:
        async with aiofiles.open(destination, 'wb') as outfile:
            for part_path in part_paths:
                logger.debug(f"Merging part file {part_path} into {destination}.")
                async with aiofiles.open(part_path, 'rb') as infile:
                    while True:
                        chunk = await infile.read(chunk_size)
                        if not chunk:
                            break
                        await outfile.write(chunk)
                        logger.debug(f"Wrote {len(chunk)} bytes from {part_path} to {destination}.")
        logger.info("All parts merged successfully.")
    except Exception as e:
        logger.error(f"Error merging files: {e}")
        raise

async def verify_checksum(file_path: Path, expected_checksum: str, algorithm: str = 'md5'):
    try:
        hash_func = getattr(hashlib, algorithm)()
    except AttributeError:
        raise ValueError(f"Unsupported checksum algorithm: {algorithm}")
    
    try:
        logger.debug(f"Starting checksum verification using {algorithm} for {file_path}.")
        async with aiofiles.open(file_path, 'rb') as f:
            while True:
                chunk = await f.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break
                hash_func.update(chunk)
        calculated = hash_func.hexdigest()
        if calculated.lower() == expected_checksum.lower():
            logger.info(f"Checksum verification passed ({algorithm}: {calculated}).")
        else:
            raise Exception(f"Checksum verification failed. Expected: {expected_checksum}, Got: {calculated}")
    except Exception as e:
        logger.error(f"Error during checksum verification: {e}")
        raise

# --------------------- Monitor Function ---------------------

async def download_success_monitor(conn_manager: ConnectionManager, ideal_connections: int, temp_file: Path):
    """
    Monitors the download process to determine when no 503 errors have occurred for 5 minutes.
    Once the condition is met, saves the current connection count as the ideal max connections.
    """
    duration = 300  # 5 minutes in seconds
    while True:
        await asyncio.sleep(duration)
        # Check if no reductions have occurred in the last 5 minutes
        async with conn_manager.lock:
            last_reduction = conn_manager.last_reduction_time
            if last_reduction is None:
                continue  # No reductions yet
            elapsed = time.time() - last_reduction
            if elapsed >= duration:
                # No reductions in the last 5 minutes
                if conn_manager.current_connections > ideal_connections:
                    # Update ideal_connections
                    ideal_connections = conn_manager.current_connections
                    await save_ideal_connections(temp_file, ideal_connections)
                logger.info(f"No 503 errors detected for {duration / 60} minutes. Ideal max connections set to {ideal_connections}.")
                break  # Exit the monitor
            else:
                remaining = duration - elapsed
                logger.info(f"Waiting {remaining:.1f} more seconds before setting ideal max connections.")

# --------------------- Main Download Function ---------------------

async def download_file(config):
    url = config['url']
    download_folder = Path(config['download_folder'])
    download_folder.mkdir(parents=True, exist_ok=True)
    file_name = get_file_name(url)
    destination = download_folder / file_name
    temp_dir = download_folder / f".{file_name}.parts"
    temp_dir.mkdir(exist_ok=True)

    logger.info("Please ensure you have sufficient storage space. The compressed file is typically over 19 GB and expands to over 86 GB when decompressed.")
    logger.info("Also check your system's file size limits, especially on 32-bit systems.")

    temp_file = get_temp_file_path()
    default_connections = config['num_connections']
    starting_connections = await get_user_preference(temp_file, default_connections)

    conn_manager = ConnectionManager(initial_connections=starting_connections, cooldown_period=config.get('connection_cooldown', 30))
    num_connections = await conn_manager.get_current_connections()

    # Start the monitor task to save ideal connections after 5 minutes of no 503 errors
    monitor_task = asyncio.create_task(download_success_monitor(conn_manager, starting_connections, temp_file))

    while num_connections >= 1:
        logger.info(f"Attempting download with {num_connections} connection(s).")
        connector = TCPConnector(limit=num_connections, force_close=True)
        timeout = ClientTimeout(total=config['timeout'])
        async with ClientSession(connector=connector, timeout=timeout) as session:
            logger.info(f"Fetching file information for {url}...")
            try:
                total_size, accept_ranges = await get_file_size(session, url)
            except Exception as e:
                logger.error(f"Unable to retrieve file information: {e}")
                sys.exit(1)

            if total_size == 0:
                logger.error("File size is 0. Exiting.")
                sys.exit(1)

            logger.info(f"File size: {total_size / (1024**3):.2f} GB")
            if not accept_ranges:
                logger.warning("Server does not support byte ranges. Falling back to single connection.")
                await conn_manager.decrease_connections()
                num_connections = await conn_manager.get_current_connections()
                continue

            part_size = math.ceil(total_size / num_connections)
            logger.info(f"Using {num_connections} connection(s) with part size {part_size / (1024**2):.2f} MB each.")

            part_paths = [temp_dir / f"part_{i}" for i in range(num_connections)]

            # Determine ranges and check for existing parts
            ranges = []
            initial_progress = 0
            for i in range(num_connections):
                start = part_size * i
                end = min(start + part_size - 1, total_size - 1)
                part_path = part_paths[i]
                
                if part_path.exists():
                    existing_size = part_path.stat().st_size
                    expected_size = end - start + 1
                    if existing_size == expected_size:
                        logger.info(f"Part {i} already fully downloaded. Skipping.")
                        initial_progress += existing_size
                        ranges.append(None)  # Indicate that this part is complete
                    elif existing_size < expected_size:
                        logger.info(f"Part {i} partially downloaded. Resuming from byte {start + existing_size}.")
                        ranges.append((start + existing_size, end))
                        initial_progress += existing_size
                    else:
                        logger.warning(f"Part {i} has unexpected size ({existing_size} bytes). Re-downloading.")
                        part_path.unlink()  # Remove corrupted part
                        ranges.append((start, end))
                else:
                    logger.info(f"Part {i} not found. Starting new download.")
                    ranges.append((start, end))

            pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc=file_name, initial=initial_progress, dynamic_ncols=True)

            # Create download tasks
            tasks = []
            for i, range_info in enumerate(ranges):
                if range_info is not None:
                    start, end = range_info
                    part_path = part_paths[i]
                    task = download_range(
                        session=session,
                        url=url,
                        start=start,
                        end=end,
                        part_path=part_path,
                        retries=config['max_retries'],
                        backoff=config['retry_backoff'],
                        progress=pbar,
                        user_agent=config['user_agent'],  # Pass user_agent to download_range
                        conn_manager=conn_manager
                    )
                    tasks.append(task)

            # Handle graceful shutdown
            loop = asyncio.get_running_loop()
            for s in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(s, partial(signal_handler, loop, s))
                    logger.debug(f"Signal handler added for {s.name}.")
                except NotImplementedError:
                    logger.warning(f"Signal handling for {s.name} is not supported on this platform.")

            # Execute download tasks
            try:
                await asyncio.gather(*tasks)
            except ServiceUnavailableError as e:
                logger.error(f"Service Unavailable encountered: {e}")
                pbar.close()
                num_connections = await conn_manager.get_current_connections()
                continue  # Retry with fewer connections
            except asyncio.CancelledError:
                logger.warning("Download tasks were cancelled.")
                pbar.close()
                sys.exit(1)
            except Exception as e:
                logger.error(f"Download failed: {e}")
                pbar.close()
                num_connections = await conn_manager.get_current_connections()
                continue  # Retry with fewer connections

            pbar.close()

            # Merge parts
            logger.info("Merging parts...")
            try:
                await merge_files(part_paths, destination)
            except Exception as e:
                logger.error(f"Failed to merge parts: {e}")
                sys.exit(1)

            # Remove temporary parts directory
            try:
                shutil.rmtree(temp_dir)
                logger.info(f"Removed temporary directory {temp_dir}.")
            except Exception as e:
                logger.warning(f"Could not remove temporary directory {temp_dir}: {e}")

            # Verify checksum if provided
            if config['checksum']:
                logger.info("Verifying checksum...")
                checksum = config['checksum']
                if ':' in checksum:
                    algorithm, expected = checksum.split(':', 1)
                else:
                    algorithm, expected = 'md5', checksum
                try:
                    await verify_checksum(destination, expected, algorithm)
                except Exception as e:
                    logger.error(f"Checksum verification failed: {e}")
                    sys.exit(1)

            logger.info(f"Download completed successfully: {destination}")
            break  # Exit the loop upon successful download

# --------------------- Signal Handler ---------------------

def signal_handler(loop, signal_received):
    logger.warning(f"Received exit signal {signal_received.name}... Initiating graceful shutdown.")
    for task in asyncio.all_tasks(loop):
        task.cancel()

# --------------------- Entry Point ---------------------

def main():
    args = parse_args()
    config = load_config(args)

    logger.debug(f"Final configuration: {config}")

    try:
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, partial(signal_handler, loop, signal.SIGINT))
        loop.add_signal_handler(signal.SIGTERM, partial(signal_handler, loop, signal.SIGTERM))
        loop.run_until_complete(download_file(config))
    except KeyboardInterrupt:
        logger.warning("Download interrupted by user.")
        sys.exit(1)
    except asyncio.CancelledError:
        logger.warning("Download tasks were cancelled.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        loop.close()

if __name__ == "__main__":
    main()