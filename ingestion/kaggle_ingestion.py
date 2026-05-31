import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

load_dotenv()  # Load environment variables from .env file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)




def get_adls_client():
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

    if not account_name or not account_key:
        raise ValueError("Missing Azure storage account name or key")

    return DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )


def upload_to_raw(local_path: str, adls_path: str, container: str = "raw"):
    client = get_adls_client()
    fs_client = client.get_file_system_client(container)
    file_client = fs_client.get_file_client(adls_path)

    with open(local_path, "rb") as f:
        data = f.read()
        file_client.upload_data(data, overwrite=True)
        logger.info(f"Uploaded {local_path} → raw/{adls_path}")


def run_kaggle_ingestion():
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    kaggle_dir = "data/raw/kaggle"
    ingestion_date = datetime.utcnow().strftime("%Y/%m/%d")

    logger.info(f"Starting Kaggle ingestion | run_id={run_id}")

    uploaded = 0
    failed = 0

    for root, dirs, files in os.walk(kaggle_dir):
        for filename in files:
            if not filename.endswith(".csv"):
                continue

            local_path = os.path.join(root, filename)
            relative = os.path.relpath(local_path, kaggle_dir)
            adls_path = f"kaggle/{ingestion_date}/{relative}".replace("\\", "/")

            try:
                upload_to_raw(local_path, adls_path)
                uploaded += 1
            except Exception as e:
                logger.error(f"Failed to upload {local_path}: {e}")
                failed += 1

    logger.info(f"Kaggle ingestion complete | uploaded={uploaded} | failed={failed} | run_id={run_id}")
    return {"run_id": run_id, "uploaded": uploaded, "failed": failed}


if __name__ == "__main__":
    result = run_kaggle_ingestion()
    print(result)