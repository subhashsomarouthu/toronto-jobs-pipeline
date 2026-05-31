import os
import json
import logging
import time
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

APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")
ACTOR_ID = "valig~linkedin-jobs-scraper"
BASE_URL = "https://api.apify.com/v2"

SEARCH_TERMS = [
    {"title": "data engineer",          "location": "Toronto, Canada"},
    {"title": "analytics engineer",     "location": "Toronto, Canada"},
    {"title": "data analyst",           "location": "Toronto, Canada"},
    {"title": "ETL developer",          "location": "Canada"},
    {"title": "data platform engineer", "location": "Canada"},
]

RESULTS_PER_TERM = 200  # 5 terms × 200 = 1000 results = $0.10 per run
CREDIT_ALERT_THRESHOLD = 1.00


def get_adls_client():
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    return DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )


def check_credit_balance() -> float:
    url = f"{BASE_URL}/users/me"
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    balance = data.get("data", {}).get("plan", {}).get("monthlyUsage", {})
    # remaining = limit - used
    limit = balance.get("monthlyServiceUsageLimitUsd", 5.0)
    used = balance.get("totalChargesThisMonth", 0.0)
    remaining = round(limit - used, 2)
    logger.info(f"Apify credit balance: ${remaining} remaining")
    return remaining


def run_actor(search_term: dict) -> str:
    url = f"{BASE_URL}/acts/{ACTOR_ID}/runs"
    headers = {
        "Authorization": f"Bearer {APIFY_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "title": search_term["title"],
        "location": search_term["location"],
        "datePosted": "r86400",
        "maxItems": RESULTS_PER_TERM
    }
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    run_id = response.json()["data"]["id"]
    logger.info(f"Actor started | run_id={run_id} | term='{search_term['title']}'")
    return run_id


def wait_for_run(run_id: str, timeout: int = 300) -> bool:
    url = f"{BASE_URL}/actor-runs/{run_id}"
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}"}
    start = time.time()

    while time.time() - start < timeout:
        response = requests.get(url, headers=headers)
        status = response.json()["data"]["status"]

        if status == "SUCCEEDED":
            logger.info(f"Actor run succeeded | run_id={run_id}")
            return True
        elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            logger.error(f"Actor run failed | run_id={run_id} | status={status}")
            return False

        logger.info(f"Waiting for run | status={status} | elapsed={int(time.time()-start)}s")
        time.sleep(10)

    logger.error(f"Actor run timed out after {timeout}s | run_id={run_id}")
    return False


def fetch_results(run_id: str) -> list:
    url = f"{BASE_URL}/actor-runs/{run_id}/dataset/items"
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def upload_to_raw(data: dict, adls_path: str, container: str = "raw"):
    client = get_adls_client()
    fs_client = client.get_file_system_client(container)
    file_client = fs_client.get_file_client(adls_path)
    content = json.dumps(data, indent=2).encode("utf-8")
    file_client.upload_data(content, overwrite=True)
    logger.info(f"Uploaded → raw/{adls_path}")


def run_apify_ingestion():
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    ingestion_date = datetime.utcnow().strftime("%Y/%m/%d")
    logger.info(f"Starting LinkedIn/Apify ingestion | run_id={run_id}")

    # Circuit breaker — check credits before spending anything
    try:
        balance = check_credit_balance()
        if balance < CREDIT_ALERT_THRESHOLD:
            logger.warning(
                f"Apify credit balance low: ${balance} "
                f"(threshold: ${CREDIT_ALERT_THRESHOLD}). "
                f"Skipping LinkedIn ingestion. Pipeline continues."
            )
            return {
                "run_id": run_id,
                "status": "skipped",
                "reason": "low_credits",
                "balance": balance
            }
    except Exception as e:
        logger.error(f"Could not check Apify balance: {e}. Skipping to be safe.")
        return {"run_id": run_id, "status": "skipped", "reason": "balance_check_failed"}

    total_jobs = 0
    failed_terms = 0

    for term in SEARCH_TERMS:
        try:
            actor_run_id = run_actor(term)
            success = wait_for_run(actor_run_id)

            if not success:
                failed_terms += 1
                continue

            jobs = fetch_results(actor_run_id)

            payload = {
                "ingestion_timestamp": datetime.utcnow().isoformat(),
                "source": "linkedin_apify",
                "pipeline_run_id": run_id,
                "actor_run_id": actor_run_id,
                "search_term": term["title"],
                "location": term["location"],
                "total_results": len(jobs),
                "results": jobs
            }

            safe_term = term["title"].replace(" ", "_")
            adls_path = f"linkedin/{ingestion_date}/{safe_term}_{run_id}.json"
            upload_to_raw(payload, adls_path)
            total_jobs += len(jobs)

        except Exception as e:
            logger.error(f"Failed ingestion for '{term['title']}': {e}")
            failed_terms += 1

    logger.info(
        f"LinkedIn/Apify ingestion complete | "
        f"total_jobs={total_jobs} | "
        f"failed_terms={failed_terms} | "
        f"run_id={run_id}"
    )
    return {
        "run_id": run_id,
        "status": "completed",
        "total_jobs": total_jobs,
        "failed_terms": failed_terms
    }


if __name__ == "__main__":
    result = run_apify_ingestion()
    print(result)