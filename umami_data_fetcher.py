#!/usr/bin/env python3
"""
Umami Data Fetcher - CLI tool for fetching Umami analytics data.

Uses click for CLI argument parsing and loguru for logging.
Version: 1.0.0
"""

import os
import sys
import csv
import json
import time
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import click
import requests
from loguru import logger
from tqdm import tqdm

from utils import __version__


class UmamiDataFetcher:
    """CLI tool for fetching Umami analytics data hour by hour."""

    def __init__(self):
        self.version = __version__
        self.metric_types = [
            "url",
            "referrer",
            "browser",
            "os",
            "device",
            "country",
            "event",
        ]

    def fetch_data(
        self,
        instance_url: str = "https://api.umami.is/v1",
        website_ids: Optional[str] = None,
        output_dir: str = "./output",
        api_key: Optional[str] = None,
        since: Optional[str] = None,
        rps: float = 1.0,
        output_format: str = "both",
    ) -> None:
        """
        Fetch Umami analytics data hour by hour.

        Parameters
        ----------
        instance_url : str
            Umami instance URL (default: https://api.umami.is/v1).
        website_ids : str, optional
            Comma-separated website IDs. If None, fetch all available.
        output_dir : str
            Directory to store CSV and log files.
        api_key : str, optional
            Umami API key. If None, load from UMAMI_API_KEY env var.
        since : str, optional
            Time period to fetch data for (e.g., "37d" for 37 days, "25h" for 25 hours).
        rps : float
            Requests per second rate limit (default: 1.0).
        output_format : str
            Output format - "csv", "json", or "both" (default: "both").
        """
        # Setup logging
        self._setup_logging(output_dir)

        # Validate and setup parameters
        output_format = self._validate_output_format(output_format)
        api_key = self._get_api_key(api_key)
        websites = self._get_website_ids(instance_url, api_key, website_ids)
        start_datetime = self._parse_since(since)

        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Check for interrupted atomic operations
        self._check_atomic_operations(output_dir)

        logger.info(
            f"Starting data fetch for {len(websites)} websites from {start_datetime}"
        )

        # Fetch data for each website
        for website in websites:
            self._fetch_website_data(
                instance_url,
                api_key,
                website,
                start_datetime,
                output_dir,
                rps,
                output_format,
            )

        logger.info("Data fetching completed successfully")

    def _setup_logging(self, output_dir: str):
        """Setup loguru logging with debug file output."""
        logger.remove()  # Remove default handler
        logger.add(sys.stderr, level="INFO")
        logger.add(
            Path(output_dir) / "umami_fetcher.log",
            level="DEBUG",
            rotation="10 MB",
            retention="30 days",
        )

    def _validate_output_format(self, output_format: str) -> str:
        """
        Validate the output_format parameter.

        Args:
            output_format: The requested output format

        Returns:
            The validated output format (lowercase)

        Raises:
            ValueError: If the format is not one of "csv", "json", or "both"
        """
        output_format = output_format.lower().strip()
        valid_formats = ["csv", "json", "both"]

        if output_format not in valid_formats:
            raise ValueError(
                f"Invalid output_format: {output_format}. Must be one of {valid_formats}"
            )

        return output_format

    def _get_api_key(self, api_key: Optional[str]) -> str:
        """Get API key from parameter or environment."""
        if api_key:
            return api_key

        api_key = os.getenv("UMAMI_API_KEY")
        if not api_key:
            raise ValueError(
                "API key must be provided via --api_key or UMAMI_API_KEY environment variable"
            )

        return api_key

    def _get_website_ids(
        self, instance_url: str, api_key: str, website_ids: Optional[str]
    ) -> List[dict]:
        """Get website information from parameter or fetch all available."""
        headers = {"x-umami-api-key": api_key, "Accept": "application/json"}

        response = requests.get(f"{instance_url}/websites", headers=headers)
        response.raise_for_status()

        data = response.json()
        websites = data.get("data", [])

        if not websites:
            raise ValueError("No websites found in the account")

        # Filter to specific IDs if provided
        if website_ids:
            requested_ids = [wid.strip() for wid in website_ids.split(",")]
            websites = [site for site in websites if site["id"] in requested_ids]

            if not websites:
                raise ValueError(
                    f"None of the requested website IDs were found: {requested_ids}"
                )

            logger.info(
                f"Found {len(websites)} of {len(requested_ids)} requested websites"
            )
        else:
            logger.info(
                f"No website IDs provided, fetching all {len(websites)} available websites"
            )

        return websites

    def _parse_since(self, since: str) -> datetime:
        """
        Parse since parameter and calculate start datetime.

        The since parameter accepts formats like "37d" (days) or "25h" (hours),
        and calculates the start datetime by subtracting that duration from now.

        Args:
            since: Time period string (e.g., "37d" or "25h")

        Returns:
            datetime: The calculated start datetime

        Raises:
            ValueError: If the since parameter is invalid or missing
        """
        if not since:
            raise ValueError("Since parameter is required (e.g., '37d' or '25h')")

        since = since.strip()

        # Extract number and unit
        if len(since) < 2:
            raise ValueError(
                "Since parameter must be in format like '37d' (days) or '25h' (hours)"
            )

        unit = since[-1].lower()
        try:
            value = int(since[:-1])
        except ValueError:
            raise ValueError(
                f"Invalid number in since parameter: {since[:-1]}. "
                "Expected format like '37d' or '25h'"
            )

        if value <= 0:
            raise ValueError(f"Since value must be positive, got: {value}")

        # Calculate start datetime based on unit
        now = datetime.now()

        if unit == "d":
            start_datetime = now - timedelta(days=value)
        elif unit == "h":
            start_datetime = now - timedelta(hours=value)
        else:
            raise ValueError(
                f"Invalid unit '{unit}'. Only 'd' (days) and 'h' (hours) are supported"
            )

        return start_datetime

    def _check_atomic_operations(self, output_dir: str):
        """Check for interrupted atomic operations and crash with helpful error."""
        temp_files = list(Path(output_dir).glob("*.tmp"))
        if temp_files:
            raise RuntimeError(
                f"Found temporary files from interrupted atomic operations: {temp_files}. "
                "Please remove these files manually and restart the script."
            )

    def _fetch_website_data(
        self,
        instance_url: str,
        api_key: str,
        website: dict,
        start_datetime: datetime,
        output_dir: str,
        rps: float,
        output_format: str,
    ):
        """Fetch data for a single website hour by hour."""
        website_id = website["id"]
        website_name = website["name"]
        website_domain = website["domain"]

        # Use createdAt as the earliest start datetime to avoid fetching data from before the website was created
        created_at_str = website["createdAt"].rstrip("Z")
        created_at = datetime.fromisoformat(created_at_str)
        actual_start_datetime = max(start_datetime, created_at)

        logger.info(
            f"Website {website_name} ({website_domain}) created at {created_at}, "
            f"fetching from {actual_start_datetime}"
        )

        base_filename = f"{website_name}_{website_domain}_{website_id}"
        csv_path = Path(output_dir) / f"{base_filename}.csv"
        json_path = Path(output_dir) / f"{base_filename}.json"

        # Load existing data if available (prioritize CSV, fall back to JSON)
        # This allows migration between formats while preserving existing data
        if output_format in ["csv", "both"]:
            existing_data = self._load_existing_data(csv_path)
        elif csv_path.exists():
            existing_data = self._load_existing_data(csv_path)
        else:
            existing_data = self._load_existing_data(json_path)

        # Calculate hours to fetch
        now = datetime.now()
        current_hour = actual_start_datetime.replace(minute=0, second=0, microsecond=0)
        hours_to_fetch = []

        while current_hour <= now:
            hour_key = current_hour.isoformat()

            # Skip if already completed with current version
            if (
                hour_key in existing_data
                and existing_data[hour_key].get("done") == "1"
                and existing_data[hour_key].get("version") == self.version
            ):
                logger.debug(
                    f"Skipping {hour_key} for {website_id} (already completed)"
                )
            else:
                hours_to_fetch.append(current_hour)

            current_hour += timedelta(hours=1)

        if not hours_to_fetch:
            logger.info(f"No new data to fetch for website {website_id}")
            return

        logger.info(
            f"Fetching {len(hours_to_fetch)} hours of data for website {website_id}"
        )

        # Fetch data with progress bar
        with tqdm(total=len(hours_to_fetch), desc=f"Website {website_id[:8]}") as pbar:
            for hour in hours_to_fetch:
                self._fetch_hour_data(
                    instance_url,
                    api_key,
                    website_id,
                    hour,
                    csv_path,
                    json_path,
                    existing_data,
                    output_format,
                )
                pbar.update(1)

                # Rate limiting
                if rps > 0:
                    time.sleep(1.0 / rps)

    def _flatten_dict(self, data: dict, prefix: str = "") -> dict:
        """
        Recursively flatten a nested dictionary.

        Nested dictionaries are flattened using underscore-separated keys.
        This allows storing complex API responses in flat CSV format.

        Args:
            data: Dictionary to flatten
            prefix: Prefix for keys (used in recursion)

        Returns:
            Flattened dictionary with underscore-separated keys

        Examples:
            {key1: {key2: value}} -> {key1_key2: value}
            {a: 1, b: {c: 2, d: {e: 3}}} -> {a: 1, b_c: 2, b_d_e: 3}
        """
        result = {}

        for key, value in data.items():
            # Build the new key with prefix
            new_key = f"{prefix}_{key}" if prefix else key

            if isinstance(value, dict):
                # Recursively flatten nested dictionaries
                result.update(self._flatten_dict(value, prefix=new_key))
            else:
                # Add the value with the flattened key
                result[new_key] = value

        return result

    def _load_existing_data(self, file_path: Path) -> dict:
        """
        Load existing data from CSV or JSON file into memory.

        Args:
            file_path: Path to the CSV or JSON file

        Returns:
            Dictionary mapping hour keys to row data
        """
        existing_data = {}

        if not file_path.exists():
            return existing_data

        try:
            if file_path.suffix == ".csv":
                with open(file_path, "r", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        hour_key = row.get("hour")
                        if hour_key:
                            existing_data[hour_key] = row
            elif file_path.suffix == ".json":
                with open(file_path, "r") as f:
                    data = json.load(f)
                    # JSON format stores data as a list of records
                    for record in data:
                        hour_key = record.get("hour")
                        if hour_key:
                            existing_data[hour_key] = record
        except Exception as e:
            logger.warning(f"Could not load existing data from {file_path}: {e}")

        return existing_data

    def _fetch_hour_data(
        self,
        instance_url: str,
        api_key: str,
        website_id: str,
        hour: datetime,
        csv_path: Path,
        json_path: Path,
        existing_data: dict,
        output_format: str,
    ):
        """Fetch data for a single hour and update output file(s) atomically."""
        start_ms = int(hour.timestamp() * 1000)
        end_ms = int((hour + timedelta(hours=1)).timestamp() * 1000)

        headers = {"x-umami-api-key": api_key, "Accept": "application/json"}

        # Fetch stats
        stats_url = f"{instance_url}/websites/{website_id}/stats"
        stats_params = {"startAt": start_ms, "endAt": end_ms}

        try:
            stats_response = requests.get(
                stats_url, headers=headers, params=stats_params
            )
            stats_response.raise_for_status()
            stats_data = stats_response.json()
        except Exception as e:
            logger.error(f"Failed to fetch stats for {hour}: {e}")
            return

        # Fetch metrics for each type
        metrics_data = {}
        for metric_type in self.metric_types:
            try:
                metrics_url = f"{instance_url}/websites/{website_id}/metrics"
                metrics_params = {
                    "startAt": start_ms,
                    "endAt": end_ms,
                    "type": metric_type,
                }

                metrics_response = requests.get(
                    metrics_url, headers=headers, params=metrics_params
                )
                metrics_response.raise_for_status()
                metrics_data[metric_type] = metrics_response.json()

                # Rate limiting between metric requests
                time.sleep(0.1)  # Small delay between metric requests

            except Exception as e:
                logger.warning(f"Failed to fetch {metric_type} metrics for {hour}: {e}")
                metrics_data[metric_type] = []

        # Determine if this hour is "done" (more than 1 hour has passed)
        now = datetime.now()
        is_done = "1" if (now - hour).total_seconds() > 3600 else "0"

        # Prepare row data with metadata
        row_data = {
            "version": self.version,
            "website_id": website_id,
            "hour": hour.isoformat(),
            "done": is_done,
        }

        # Flatten and add all stats data dynamically to preserve all API fields regardless of changes
        # This handles nested structures like {key1: {key2: value}} -> {key1_key2: value}
        flattened_stats = self._flatten_dict(stats_data)
        for stat_key, stat_value in flattened_stats.items():
            row_data[stat_key] = stat_value

        # Add metrics data as JSON strings
        for metric_type in self.metric_types:
            row_data[f"metrics_{metric_type}"] = json.dumps(
                metrics_data.get(metric_type, [])
            )

        # Update existing data and write atomically in requested format(s)
        hour_key = hour.isoformat()
        existing_data[hour_key] = row_data

        if output_format in ["csv", "both"]:
            self._write_csv_atomic(csv_path, existing_data)

        if output_format in ["json", "both"]:
            self._write_json_atomic(json_path, existing_data)

        logger.debug(f"Updated data for {hour_key}, done={is_done}")

    def _write_csv_atomic(self, csv_path: Path, data: dict):
        """
        Write CSV data atomically using temporary file.

        The data is written to a temporary file first, then atomically
        moved to the final location to prevent corruption if interrupted.

        Args:
            csv_path: Path to the CSV file
            data: Dictionary mapping hour keys to row dictionaries
        """
        temp_path = csv_path.with_suffix(".tmp")

        try:
            with open(temp_path, "w", newline="") as f:
                if not data:
                    return

                fieldnames = list(next(iter(data.values())).keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

                # Sort by hour for consistent output
                sorted_data = sorted(data.values(), key=lambda x: x["hour"])
                writer.writerows(sorted_data)

            # Atomic move
            temp_path.replace(csv_path)

        except Exception as e:
            # Clean up temp file on error
            if temp_path.exists():
                temp_path.unlink()
            raise e

    def _write_json_atomic(self, json_path: Path, data: dict):
        """
        Write JSON data atomically using temporary file.

        The data is written to a temporary file first, then atomically
        moved to the final location to prevent corruption if interrupted.
        Unlike CSV, JSON preserves nested structures without flattening.

        For metrics fields that are stored as JSON strings (for CSV compatibility),
        we parse them back to native objects to avoid unnecessary escaping in JSON.

        Args:
            json_path: Path to the JSON file
            data: Dictionary mapping hour keys to record dictionaries
        """
        temp_path = json_path.with_suffix(".tmp")

        try:
            with open(temp_path, "w") as f:
                if not data:
                    json.dump([], f, indent=2)
                    return

                # Convert metrics from JSON strings to objects for native JSON storage
                # This avoids unnecessary escaping in the JSON output
                processed_data = []
                for record in data.values():
                    record_copy = record.copy()

                    # Parse JSON strings back to objects for metrics fields
                    for metric_type in self.metric_types:
                        metric_key = f"metrics_{metric_type}"
                        if metric_key in record_copy:
                            try:
                                record_copy[metric_key] = json.loads(
                                    record_copy[metric_key]
                                )
                            except (json.JSONDecodeError, TypeError):
                                # Keep as-is if parsing fails
                                pass

                    processed_data.append(record_copy)

                # Sort by hour for consistent output
                sorted_data = sorted(processed_data, key=lambda x: x["hour"])
                json.dump(sorted_data, f, indent=2)

            # Atomic move
            temp_path.replace(json_path)

        except Exception as e:
            # Clean up temp file on error
            if temp_path.exists():
                temp_path.unlink()
            raise e


@click.command()
@click.option(
    "--instance-url",
    default="https://api.umami.is/v1",
    show_default=True,
    help="Umami instance URL.",
)
@click.option(
    "--website-ids",
    default=None,
    help="Comma-separated website IDs. If omitted, fetch all available.",
)
@click.option(
    "--output-dir",
    default="./output",
    show_default=True,
    help="Directory to store CSV and log files.",
)
@click.option(
    "--api-key",
    default=None,
    help="Umami API key. Falls back to UMAMI_API_KEY env var.",
)
@click.option(
    "--since",
    required=True,
    help="Time period to fetch (e.g. '37d' for 37 days, '25h' for 25 hours).",
)
@click.option(
    "--rps",
    default=1.0,
    show_default=True,
    help="Requests per second rate limit.",
)
@click.option(
    "--output-format",
    type=click.Choice(["csv", "json", "both"], case_sensitive=False),
    default="both",
    show_default=True,
    help="Output format.",
)
def main(
    instance_url: str,
    website_ids: Optional[str],
    output_dir: str,
    api_key: Optional[str],
    since: str,
    rps: float,
    output_format: str,
) -> None:
    """Fetch Umami analytics data hour by hour."""
    fetcher = UmamiDataFetcher()
    fetcher.fetch_data(
        instance_url=instance_url,
        website_ids=website_ids,
        output_dir=output_dir,
        api_key=api_key,
        since=since,
        rps=rps,
        output_format=output_format,
    )


if __name__ == "__main__":
    main()
