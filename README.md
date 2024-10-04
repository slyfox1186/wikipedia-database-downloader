# Wikipedia Database Downloader

An efficient and reliable Python script for downloading Wikipedia database dumps. This tool supports multi-threaded downloads, resume functionality, and automatic retry with connection reduction in case of server errors.

## Features

- 🚀 Multi-threaded downloading for faster speeds
- ⏸️ Resume capability for interrupted downloads
- 🔄 Automatic retry with connection reduction on 503 errors
- ✅ Checksum verification
- 🛠️ Configurable through command-line arguments or a YAML config file
- 📊 Detailed logging
- 🔍 Adaptive connection management
- 🔒 User agent customization for ethical scraping
- 📦 Automatic part size calculation based on file size
- 🔁 Cooldown period for connection reductions to prevent excessive retries
- 📈 Optimal connection discovery and saving for future downloads
- 🧩 File splitting and merging for efficient downloading and storage
- 🔔 Signal handling for graceful shutdown
- 💾 Temporary file management for download parts
- 🔄 Automatic connection optimization
- 📊 Progress tracking and reporting
- 🔍 File size and range support detection
- 🔐 Secure temporary file handling
- 🔄 Intelligent retry mechanism with exponential backoff and jitter
- 📈 Dynamic connection adjustment based on server response
- 🧠 Smart resumption of partially downloaded files
- 🔍 Detailed error handling and reporting
- 🔄 Asynchronous I/O for improved performance

## Requirements

- Python 3.7+
- Required Python packages:
  - aiohttp
  - aiofiles
  - tqdm
  - PyYAML

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/wikipedia-database-downloader.git
   cd wikipedia-database-downloader
   ```

2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## Configuration

You can configure the script using command-line arguments or a YAML configuration file. If not specified, default values will be used.

### Command-line Arguments

| Argument | Description |
|----------|-------------|
| `--config` | Path to YAML configuration file |
| `--url` | URL of the Wikipedia dump |
| `--folder` | Download folder path |
| `--connections` | Number of concurrent connections |
| `--chunk-size` | Chunk size for downloading (in bytes) |
| `--max-retries` | Maximum number of retries for failed downloads |
| `--retry-backoff` | Backoff factor for retries |
| `--timeout` | Timeout for HTTP requests in seconds |
| `--checksum` | Expected checksum of the file (e.g., MD5, SHA256) |
| `--user-agent` | Custom user agent string for the download request |
| `--max-average-parts` | Maximum size per part in megabytes |

### YAML Configuration File

Create a YAML file with the following structure:

```
url: 'https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2'
download_folder: './database_files'
num_connections: 16
chunk_size: 1048576
max_retries: 20
retry_backoff: 2.0
timeout: 30
checksum: 'md5:checksum_value_here'
user_agent: 'Your Custom User Agent'
connection_cooldown: 10
optimal_connection_timeout: 300
increase_failure_limit: 2
increase_wait_time: 900
max_average_parts: 10
```

## Usage

Run the script with:

```
python download_wikipedia.py [arguments]
```

Example:

```
python download_wikipedia.py --config config.yaml --connections 8
```

## Advanced Features

### Adaptive Connection Management

The script automatically adjusts the number of connections based on server responses. If 503 errors are encountered, it will reduce the number of connections and retry.

### Optimal Connection Discovery

The script attempts to find the optimal number of connections for each download. It saves this information for future use with the same URL.

### Resume Functionality

If a download is interrupted, the script can resume from where it left off, saving time and bandwidth.

### Checksum Verification

When provided with a checksum, the script verifies the integrity of the downloaded file.

### Detailed Logging

Comprehensive logging provides insights into the download process, errors, and optimizations.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Wikipedia for providing free access to their database dumps
- The Python community for the excellent libraries used in this project

## Disclaimer

Please use this tool responsibly and in accordance with Wikipedia's terms of service and download guidelines.
