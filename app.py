import os
import logging
import time
import requests
from flask import Flask, request, jsonify
from dotenv import load_dotenv

app = Flask(__name__)

ENRICHMENT_URL = os.getenv("ENRICHMENT_URL")
ANALYTICS_URL = os.getenv("ANALYTICS_URL")
AUTH_HEADER = {"Authorization": "eye-am-hiring"}

enriched_records = []
non_enriched_records = []
failed_records = []
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

LAST_SENT_MESSAGE_TIME = 0
RATE_LIMIT_INTERVAL = 10


def enrich_record(record, max_retries=3, retry_wait=0.5):
    """
    Send enriched records to analytics.
    Args:
        record (dict): The dictionary containing the CSV row data to be sent to the microservice.
        max_retries : number of retries to enrich a record when the enrichment fails
        retry_wait : waiting time between retries
    """
    retries = 0
    while retries < max_retries:
        try:
            response = requests.post(
                url=ENRICHMENT_URL, json=record, headers=AUTH_HEADER, timeout=10
            )
            response.raise_for_status()

            enrichment_data = response.json()
            record.update(enrichment_data)  # Merge the enrichment data into the record
            return record, 200  # Success

        except requests.exceptions.HTTPError as err:
            logger.error(
                f"Enrichment service HTTP error: {err}, retrying {retries + 1}/{max_retries}"
            )
            retries += 1

        except requests.exceptions.Timeout as err:
            logger.error(f"Timeout error: {err}, retrying {retries + 1}/{max_retries}")
            retries += 1

        time.sleep(retry_wait)

    # If it fails after max retries, add to failed_records for future retry
    logger.error(f"Failed to enrich record after {max_retries} attempts: {record}")
    failed_records.append(record)
    return None, 500


def send_to_analytics_service(record):
    """
    Send enriched records to analytics.
    Args:
        record (dict): The dictionary containing the CSV row data to be sent to the microservice.
    """
    global LAST_SENT_MESSAGE_TIME
    current_time = time.time()
    time_since_last = current_time - LAST_SENT_MESSAGE_TIME
    if time_since_last < RATE_LIMIT_INTERVAL:
        time.sleep(RATE_LIMIT_INTERVAL - time_since_last)

    try:
        response = requests.post(
            url=ANALYTICS_URL, json=record, headers=AUTH_HEADER, timeout=10
        )
        response.raise_for_status()
        LAST_SENT_MESSAGE_TIME = time.time()
        return response.json(), 200
    except requests.exceptions.HTTPError as err:
        logger.error(f"Analytics service HTTP error: {err}")
        return None, 500
    except requests.exceptions.Timeout as err:
        logger.error(f"Analytics service timeout error: {err}")
        return None, 408


def retry_failed_records():
    """
    Retry enriching all failed records in the `failed_records` queue.
    """
    if not failed_records:
        return

    logger.info(f"Retrying {len(failed_records)} failed records...")
    records_to_retry = failed_records.copy()
    for record in records_to_retry:
        enriched_record, status_code = enrich_record(record)
        if status_code == 200:
            failed_records.remove(record)  # Remove successfully enriched record
            enriched_records.append(enriched_record)
        else:
            logger.error(f"Failed to enrich record again: {record}")


@app.route("/process_record", methods=["POST"])
def process_record():
    record = request.json

    enriched_record, status_code = enrich_record(record)
    if status_code == 200:
        enriched_records.append(enriched_record)
    else:
        non_enriched_records.append(record)

    # Check if enriched_records reaches 20, send to analytics
    if len(enriched_records) >= 20:
        analytics_response, status_code = send_to_analytics_service(
            enriched_records[:20]
        )
        enriched_records.clear()  # Clear only the sent records

        if status_code == 200:
            retry_failed_records()  # Retry failed records after sending current batch
            return (
                jsonify(
                    {"status": "success", "analytics_response": analytics_response}
                ),
                200,
            )

        return (
            jsonify({"status": "failure", "message": "Failed to send to analytics"}),
            500,
        )

    return jsonify({"status": "success", "message": "Records processed"}), 200


if __name__ == "__main__":
    load_dotenv()
    app.run(host="0.0.0.0", port=5000)
