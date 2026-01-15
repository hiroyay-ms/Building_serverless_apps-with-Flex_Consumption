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

@app.route(route="hello")
def hello(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}!")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
