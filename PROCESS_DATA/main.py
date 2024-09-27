from fastapi import APIRouter
from fastapi.responses import JSONResponse
import logging

from PROCESS_DATA import helper
from PROCESS_DATA.models import DataMinionPathInfo, TableInfoRequest, InsertRequest
from fastapi import Depends
from security import validate_token

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.INFO)

router = APIRouter()


@router.post("/data/get-data-parquet-files", tags=["DATA"])
def get_data_parquet_files(request: DataMinionPathInfo):
    try:
        return JSONResponse(status_code=200, content=helper.get_data_from_parquet(request))
    except Exception as e:
        return JSONResponse(status_code=400, content={"message": f"{str(e)}"})


@router.post("/data/create-external-table", tags=["DATA"])
def create_external_table(request: TableInfoRequest):
    try:
        return JSONResponse(status_code=200, content=helper.create_external_table(request))
    except Exception as e:
        return JSONResponse(status_code=400, content={"message": f"{str(e)}"})


@router.post("/data/insert-data", tags=["DATA"])
def insert(request: InsertRequest):
    try:
        return JSONResponse(status_code=200, content=helper.insert_data(request))
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": f"{str(e)}"})
