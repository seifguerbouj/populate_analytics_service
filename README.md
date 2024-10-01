# populate_analytics_service

## Overview
This project enables the Cyber Security Operations team to ingest, enrich, and send malicious activity logs to the Analytics Service. The system is composed of:
- A **CLI Application** that reads CSV files and optionally filters records.
- A **Microservice API** to process records, enrich data, and send them to the Analytics Service.
- **Rate-limited communication** with the Analytics Service (20 records every 10 seconds).

### Environment variables
A .env file needs to be created for the code to run.  
The environmet variables to add are :
- MICROSERVICE_PATH (which is the rest api host:port which is the default for now)
- ENRICHMENT_URL
- ANALYTICS_URL


### Running the code
in the populate_analytics_service directory
```shell
pipenv install --skip-lock && pipenv shell
```
#### Flask app

```shell
flask run
```
#### Cli ingestion
```shell
python cli_ingestion.py <file_name> --filter <optional>
```
