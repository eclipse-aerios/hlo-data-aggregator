'''
Docstring
'''
from fastapi import FastAPI
from app.routers import router

# FastAPI object customization
FASTAPI_TITLE = "IE-Resource-Alert-EP"
FASTAPI_DESCRIPTION = "HLO component, part of Data-Aggregator, to receive alert and re-orchestration events"
FASTAPI_VERSION = "0.109.0"
FASTAPI_OPEN_API_URL = "/docs"
FASTAPI_DOCS_URL = "/"

app = FastAPI(title=FASTAPI_TITLE,
              description=FASTAPI_DESCRIPTION,
              version=FASTAPI_VERSION,
              docs_url=FASTAPI_DOCS_URL,
              openapi_url=FASTAPI_OPEN_API_URL)

app.include_router(router=router, tags=["ie-alert-ep"])