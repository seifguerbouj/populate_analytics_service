import time
import csv
import argparse
import os
import logging
import requests

from dotenv import load_dotenv


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_arguments():
    
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "csv_to_ingest", type=str, help="Path to the csv file to ingest"
    )
    parser.add_argument(
        "--filter", nargs="+", help="Select the records to be ingested", default=None
    )

    return parser.parse_args()


def check_filter_values(row: dict, filter_values: list):
    
    for filter_value in filter_values:
        if filter_value in row.values():
            return True
    return False


def read_csv_file(file_path: str, filter_values: list = None, batch_to_yield=20):
    

    batch = []
    # encoding because of Byte order mark
    with open(file=file_path, mode="r", encoding="utf8") as csvfile:
        reader = csv.DictReader(f=csvfile, delimiter=";")
        for row in reader:
            if "created_utc" in row:
                del row["created_utc"]
            if "source" in row:
                del row["source"]
            if "created_utc" in row:
                row["asset"] = row.pop("asset_name")

            if filter_values is None or check_filter_values(
                row=row, filter_values=filter_values
            ):
                batch.append(row)
                #better for performance because we don't need to read the whole file in memory
                if len(batch) == batch_to_yield:
                    yield batch
                    batch = []


def send_request_to_microservice(records):
    
    microservice_path = os.getenv("MICROSERVICE_PATH")
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    try:
        response = requests.post(
            url=microservice_path, json=records, timeout=10, headers=headers
        )
        response.raise_for_status()
        logger.info(
            "Processed successfully record ID %s:", [record["id"] for record in records]
        )
        return True
    except requests.exceptions.HTTPError as err:
        logger.error(
            "Failed processing record ID %s: ", [record["id"] for record in records]
        )
        logger.error("http error: %s ", {err})
        return False
    except requests.exceptions.Timeout:
        logger.error(
            "Timeout occured when processing record ID : %s",
            [record["id"] for record in records],
        )
        return False


def main():
    load_dotenv()

    args = parse_arguments()
    success_count = 0
    total_count = 0

    for record in read_csv_file(args.csv_to_ingest, args.filter):
        total_count += len(record)
        success = send_request_to_microservice(records=record)
        if success:
            success_count += len(record)
        # sleep to not overwhelm the service
        time.sleep(0.5)
    logger.info(
        "Ingestion completed.  %d/%d records processed", success_count, total_count
    )


if __name__ == "__main__":
    main()
