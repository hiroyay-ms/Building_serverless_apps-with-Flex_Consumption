import logging
import os
import json
import asyncio
from io import BytesIO
from datetime import datetime, timezone
import pandas as pd
import aiohttp
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import BlobServiceClient
import azure.functions as func

app = func.FunctionApp()

STORAGE_ACCOUNT = os.environ["STORAGE_ACCOUNT"]
BLOB_URL = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"

source_container_name = os.environ["SOURCE_CONTAINER_NAME"]
target_container_name = os.environ["TARGET_CONTAINER_NAME"]

FILTER_DATE_STR = os.getenv("FILTER_DATE", "2025-01-01")
FILTER_DATE = datetime.strptime(FILTER_DATE_STR, "%Y-%m-%d")

MAX_DOCS = 25
MAX_CHARS = 5000


async def read_filter_date_from_blob(blob_service_client):
    date_container_client = blob_service_client.get_container_client("date")
    blob_client = date_container_client.get_blob_client("last_run.json")
    try:
        stream = await blob_client.download_blob()
        data = (await stream.readall()).decode("utf-8")
        json_data = json.loads(data)
        last_run_str = json_data.get("last_run")
        logging.info(f"前回実行日: {last_run_str}, 件数: {json_data.get('processed_count')}, ステータス: {json_data.get('status')}")

        last_run_str_iso = last_run_str.replace("Z", "+00:00")
        logging.info(f"ISOフォーマット変換後: {last_run_str_iso}")

        return datetime.fromisoformat(last_run_str_iso)
    except Exception as e:
        logging.warning(f"last_run.json 読み込み失敗、デフォルト日付を使用: {e}")
        return datetime(2025, 1, 1, tzinfo=timezone.utc)


async def update_filter_date_in_blob(blob_service_client, new_date: datetime, processed_count: int, status: str):
    date_container_client = blob_service_client.get_container_client("date")
    blob_client = date_container_client.get_blob_client("last_run.json")

    last_run_str = new_date.isoformat().replace("+00:00", "Z")
    logging.info(f"新しい実行日: {last_run_str}, 件数: {processed_count}, ステータス: {status}")
    
    content = json.dumps({
        "last_run": last_run_str,
        "processed_count": processed_count,
        "status": status
    }, ensure_ascii=False, indent=2)
    await blob_client.upload_blob(content, overwrite=True)
    logging.info(f"last_run.json を更新しました: {content}")


async def merge_and_mask_batch_blob_flat(incident_blob_name: str, journal_blob_name: str):
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=BLOB_URL, credential=credential)

    filter_date = await read_filter_date_from_blob(blob_service_client)

    source_container_client = blob_service_client.get_container_client(source_container_name)
    target_container_client = blob_service_client.get_container_client(target_container_name)

    # TODO: 実際の処理を実装し、processed_count と status を設定
    processed_count = 0
    status = "success"

    await update_filter_date_in_blob(blob_service_client, datetime.now(timezone.utc), processed_count, status)

    await blob_service_client.close()
    await credential.close()
    logging.info("全件処理完了（HTTPトリガー版）")

    return {"processed_count": processed_count, "status": status}


@app.route(route="HttpETL", auth_level=func.AuthLevel.ANONYMOUS)
async def HttpETL(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("HTTPトリガー起動")
    incident_blob = req.params.get("incident_blob")
    journal_blob = req.params.get("journal_blob",)

    try:  
        result = await merge_and_mask_batch_blob_flat(incident_blob, journal_blob)
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200
        )
    except Exception as e:
        logging.exception("処理中にエラー")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}, ensure_ascii=False),
            status_code=500
        )