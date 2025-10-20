# Umami Data Fetcher

A CLI tool for backing up your Umami analytics data hour by hour.

## Why This Tool?

[Umami](https://umami.is/) is a fantastic, privacy-respecting web analytics platform. However, their free Cloud tier only retains data for 6 months. This tool allows you to backup your analytics data locally before it expires, giving you long-term access to your website statistics.

## Features

- **Incremental Backups**: Only fetches new data, skipping hours already downloaded
- **Atomic Operations**: Uses temporary files to prevent data corruption if interrupted
- **Multiple Output Formats**: Save data as CSV, JSON, or both
- **Rate Limiting**: Configurable requests per second to respect API limits
- **Progress Tracking**: Visual progress bars for each website
- **Automatic Resumption**: Detects and resumes interrupted downloads
- **Comprehensive Logging**: Debug logs saved to file for troubleshooting
- **Multi-Website Support**: Fetch data for specific websites or all at once

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

Fetch the last 37 days of data for all your websites:

```bash
./umami_data_fetcher.py fetch_data --since=37d
```

### Authentication

Provide your Umami API key either via command line or environment variable:

```bash
# Via environment variable (recommended)
export UMAMI_API_KEY="your-api-key-here"
./umami_data_fetcher.py fetch_data --since=37d

# Via command line
./umami_data_fetcher.py fetch_data --since=37d --api_key="your-api-key-here"
```

### Advanced Options

```bash
# Fetch specific websites only
./umami_data_fetcher.py fetch_data \
    --since=90d \
    --website_ids="website-id-1,website-id-2"

# Use custom Umami instance
./umami_data_fetcher.py fetch_data \
    --since=30d \
    --instance_url="https://your-umami-instance.com/api"

# Change output directory
./umami_data_fetcher.py fetch_data \
    --since=60d \
    --output_dir="./backups"

# Adjust rate limiting (default: 1 request/second)
./umami_data_fetcher.py fetch_data \
    --since=30d \
    --rps=2.0

# Choose output format (csv, json, or both)
./umami_data_fetcher.py fetch_data \
    --since=30d \
    --output_format=json
```

### Time Period Formats

The `--since` parameter accepts:
- `37d` - Days (e.g., 37 days ago)
- `25h` - Hours (e.g., 25 hours ago)

## Output Format

### CSV Format

Data is saved as one CSV file per website with flattened column structure:

- `version` - Tool version used to fetch data
- `website_id` - Umami website ID
- `hour` - ISO 8601 timestamp for the hour
- `done` - Whether the hour is complete ("1") or may receive updates ("0")
- Stats fields (flattened from API response)
- `metrics_*` - JSON-encoded metrics for each type (url, referrer, browser, os, device, country, event)

### JSON Format

Data is saved as one JSON file per website with nested structure preserving the original API response format. Metrics are stored as native JSON arrays rather than strings.

### Atomic Operations

The tool uses temporary files during writes to prevent corruption:
- If interrupted, you may find `.tmp` files in the output directory
- The tool will detect these on restart and ask you to remove them manually
- This ensures your data files are never left in a partially-written state

## Data Retention Strategy

- Hours less than 1 hour old are marked as `done=0` and will be re-fetched on subsequent runs (to capture final pageviews)
- Hours more than 1 hour old are marked as `done=1` and skipped on future runs
- You can safely run the tool multiple times - it only fetches missing or incomplete data

## Getting Your API Key

1. Log into your Umami Cloud account
2. Go to Settings → API Keys
3. Create a new API key with appropriate permissions
4. Copy the key and set it as the `UMAMI_API_KEY` environment variable

## Requirements

- Python 3.7+
- See `requirements.txt` for package dependencies

## Development

This project was developed with assistance from [aider.chat](https://github.com/Aider-AI/aider/) (more specifically, I used my [AiderBuilder](https://github.com/thiswillbeyourgithub/AiderBuilder/) agent).

## License

[Your license here]

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
