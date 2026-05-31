import os
import json
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")
BASE_URL = "https://api.adzuna.com/v1/api/jobs/ca/search"

SEARCH_TERMS = [
    "data engineer",
    "data analyst",
    "analytics engineer",
    "ETL developer",
    "data platform engineer"
]


def get_adls_client():
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    return DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )


def fetch_jobs(search_term: str, page: int = 1, results_per_page: int = 50):
    url = f"{BASE_URL}/{page}"
    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "results_per_page": results_per_page,
        "what": search_term,
        "where": "canada",
        "content-type": "application/json"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def fetch_all_pages(search_term: str, max_pages: int = 5):
    all_jobs = []
    for page in range(1, max_pages + 1):
        try:
            data = fetch_jobs(search_term, page=page)
            jobs = data.get("results", [])
            if not jobs:
                logger.info(f"No more results at page {page} for '{search_term}'")
                break
            all_jobs.extend(jobs)
            logger.info(f"Fetched page {page} for '{search_term}' — {len(jobs)} jobs")
        except Exception as e:
            logger.error(f"Failed page {page} for '{search_term}': {e}")
            break
    return all_jobs


def upload_to_raw(data: dict, adls_path: str, container: str = "raw"):
    client = get_adls_client()
    fs_client = client.get_file_system_client(container)
    file_client = fs_client.get_file_client(adls_path)
    content = json.dumps(data, indent=2).encode("utf-8")
    file_client.upload_data(content, overwrite=True)
    logger.info(f"Uploaded → raw/{adls_path}")


def run_adzuna_ingestion():
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    ingestion_date = datetime.utcnow().strftime("%Y/%m/%d")
    logger.info(f"Starting Adzuna ingestion | run_id={run_id}")

    total_jobs = 0
    failed_terms = 0

    for term in SEARCH_TERMS:
        try:
            jobs = fetch_all_pages(term)
            if not jobs:
                continue

            payload = {
                "ingestion_timestamp": datetime.utcnow().isoformat(),
                "source": "adzuna",
                "pipeline_run_id": run_id,
                "search_term": term,
                "total_results": len(jobs),
                "results": jobs
            }

            safe_term = term.replace(" ", "_")
            adls_path = f"adzuna/{ingestion_date}/{safe_term}_{run_id}.json"
            upload_to_raw(payload, adls_path)
            total_jobs += len(jobs)

        except Exception as e:
            logger.error(f"Failed ingestion for term '{term}': {e}")
            failed_terms += 1

    logger.info(
        f"Adzuna ingestion complete | "
        f"total_jobs={total_jobs} | "
        f"failed_terms={failed_terms} | "
        f"run_id={run_id}"
    )
    return {
        "run_id": run_id,
        "total_jobs": total_jobs,
        "failed_terms": failed_terms
    }


if __name__ == "__main__":
    result = run_adzuna_ingestion()
    print(result)