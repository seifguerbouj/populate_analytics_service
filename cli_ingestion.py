import time
import csv
import argparse
import os
import logging
import re
import requests
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """
    Parse the command line arguments for the CLI

    Returns:
        argparse.Namespace: Parsed arguments containing the CSV file path and optional filters.

    """
    parser = argparse.ArgumentParser(
        description="CLI to ingest a CSV file for processing."
    )
    parser.add_argument(
        "csv_to_ingest", type=str, help="Path to the CSV file to ingest"
    )
    parser.add_argument(
        "--filter",
        nargs="+",
        help="Filter records to be ingested based on values",
        default=None,
    )
    return parser.parse_args()


def check_filter_values(row: dict, filter_values: list):
    """
    Checks if filter values are in the row values

    Args:
        row (dict): The current row of the csv file.
        filter_values (list of str): List of filter values to match against the CSV rows

    Returns:
        bool: True if filter value is in row otherwise False
    """
    for filter_value in filter_values:
        if filter_value in row.values():
            return True
    return False


def read_csv_file(file_path: str, filter_values: list = None):
    """
    Read a CSV file and optionally filter records based on provided filter values.

    Args:
        file_path (str): The path to the CSV file.
        filter_values (list of str, optional): List of filter values to match against the CSV rows.
        If None, no filtering is applied.

    Yields:
        dict: A dictionary representing a row in the CSV file where any filter value is matched.
    """

    valid_category = [
        "contentinjection",
        "drivebycompromise",
        "exploitpublicfacingapplication",
        "externalremoteservices",
        "hardwareadditions",
        "phishing",
        "replicationthroughremovablemedia",
        "supplychaincompromise",
        "trustedrelationship",
        "validaccounts",
    ]
    with open(file=file_path, mode="r", encoding="utf8") as csvfile:
        reader = csv.DictReader(f=csvfile, delimiter=";")
        for row in reader:
            if "created_utc" in row:
                del row["created_utc"]
            if "source" in row:
                del row["source"]
            if "asset_name" in row:
                row["asset"] = row.pop("asset_name")
            if "id" in row:
                row["id"] = int(row.pop("id"))

            if "category" in row and isinstance(row["category"], str):
                row["category"] = re.sub("[^A-Za-z]", "", row["category"]).lower()

            if row["category"] not in valid_category:
                logger.debug(f"Skipping invalid category: {row['category']}")
                continue

            if filter_values is None or check_filter_values(
                row=row, filter_values=filter_values
            ):
                logger.debug(f"Record passed filter: {row['id']}")
                yield row
            else:
                logger.debug(f"Record filtered out: {row['id']}")


def send_request_to_microservice(record):
    """
    Send a single record to the Microservice API for processing.

    Args:
        record (dict): The dictionary containing the CSV row data to be sent to the microservice.

    Returns:
        bool: True if the record was successfully processed, False otherwise.
    """
    microservice_path = os.getenv("MICROSERVICE_PATH")
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    try:
        response = requests.post(
            url=microservice_path, json=record, timeout=10, headers=headers
        )
        response.raise_for_status()
        logger.info(f"Successfully processed record ID: {record['id']}")
        return True
    except requests.exceptions.HTTPError:
        logger.error(
            f"HTTP error while processing record ID {record['id']}: {response.content}"
        )
        return False
    except requests.exceptions.Timeout:
        logger.error(f"Timeout error for record ID: {record['id']}")
        return False


def main():
    load_dotenv()

    args = parse_arguments()
    success_count = 0
    total_count = 0

    for record in read_csv_file(args.csv_to_ingest, args.filter):
        total_count += 1
        success = send_request_to_microservice(record=record)
        if success:
            success_count += 1
        else:
            logger.error(f"Failed to process record ID: {record['id']}")
        time.sleep(0.5)  # To prevent overwhelming the service

    logger.info(
        f"Ingestion completed. {success_count}/{total_count} records processed successfully."
    )


if __name__ == "__main__":
    main()
