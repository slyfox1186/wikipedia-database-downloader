# Wikipedia Dump Downloader

An efficient and reliable Python script for downloading Wikipedia database dumps. This tool supports multi-threaded downloads, resume functionality, and automatic retry with connection reduction in case of server errors.

## Features

- 🚀 Multi-threaded downloading for faster speeds
- ⏸️ Resume capability for interrupted downloads
- 🔄 Automatic retry with connection reduction on 503 errors
- ✅ Checksum verification
- 🛠️ Configurable through command-line arguments or a YAML config file
- 📊 Detailed logging
- 🔍 Adaptive connection management

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
   git clone https://github.com/yourusername/wikipedia-dump-downloader.git
   cd wikipedia-dump-downloader
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

### YAML Configuration File

Create a YAML file with the following structure:
