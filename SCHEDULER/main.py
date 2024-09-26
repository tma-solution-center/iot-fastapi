from fastapi import FastAPI, HTTPException, APIRouter
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.cron import CronTrigger
from pydantic import BaseModel
from typing import Optional, Dict
from minio import Minio
from minio.error import S3Error
import json
import datetime
import io

from sqlalchemy import create_engine
import uvicorn
import requests
from types import SimpleNamespace
from fastapi.responses import JSONResponse
from SCHEDULER.models import *

from fastapi import Depends
from security import validate_token


router = APIRouter()
# app = FastAPI()
scheduler = BackgroundScheduler()
scheduler.start()

# *****
# Configure PostgreSQL connection
# DATABASE_URL = "postgresql://myuser:mypassword@postgres:5432/mydatabase"
DATABASE_URL = "postgresql://admin:admin@postgresql.postgresql.svc.cluster.local:5432/postgres"

# Create an SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Setup APScheduler to use PostgreSQL for job storage
scheduler = BackgroundScheduler(jobstores={
    'default': SQLAlchemyJobStore(engine=engine)
})
scheduler.start()
# *****

@router.post("/schedule", dependencies=[Depends(validate_token)], tags=["SCHEDULER"])
def schedule_job(job_data: JobRequest):
    # Kiểm tra xem hàm có tồn tại không
    if job_data.function not in globals():
        raise HTTPException(status_code=404, detail="Function not found")
    
    # Xử lý trigger type
    if job_data.trigger == "cron":
        trigger = CronTrigger(**job_data.trigger_args)
    else:
        raise HTTPException(status_code=400, detail="Unsupported trigger type")
    
    # Tạo job và thêm vào scheduler
    job = scheduler.add_job(
        globals()[job_data.function],
        trigger,
        id=job_data.name,
        kwargs=job_data.func_kwargs
    )

    return {"message": "Job scheduled successfully", "job_id": job.id}



# # Configure MinIO client (replace with your credentials)
# minio_client  = Minio(
# "localhost:9000",
# access_key="LBnu8SFjQ5o9HHkKZRtA",
# secret_key="crMri4xFFCYtGBnuX1KMU2fX9dVSmzKY4NCmroRw",
# secure=False  # Set to True if using HTTPS
# )

# bucket_name = "cdp"


@router.post("/submit_spark_job", dependencies=[Depends(validate_token)], tags=["SCHEDULER"])
def submit_spark_job(livy_request: LivySession):
    livy_request = SimpleNamespace(**livy_request) if isinstance(livy_request, dict) else livy_request
    livy_url = livy_request.livy_url if livy_request.livy_url != "" else "http://livy:8998"

    body = {
        "file": f"local://{livy_request.job_path}",
        "name": livy_request.livy_session_name,
        "executorMemory": livy_request.executor_memory,
        "args": livy_request.args
    }
    
    headers = dict()
    # headers["accept"] = "application/json"
    headers["Content-Type"] = "application/json"
    body = json.dumps(body) # covert ' -> "

    try:
        response = requests.post(f"{livy_url}/batches", data=body, headers=headers)
        return JSONResponse(status_code=response.status_code, content={"message": response.json()})
    except Exception as e:
        return JSONResponse(content={"message": str(e)})



# Xem các job đã lên lịch
@router.get("/jobs", tags=["SCHEDULER"])
def get_jobs():
    # return {name: job.id for name, job in jobs.items()}
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "job_id": job.id,
            "next_run_time": str(job.next_run_time)
        })
    return jobs

# Xóa một job theo tên
@router.delete("/jobs/{job_id}")
def delete_job(job_id: str):
    try:
        scheduler.remove_job(job_id)
        return {"message": f"Job {job_id} removed successfully"}
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found: {str(e)}")


# if __name__ == '__main__':
#     uvicorn.run(app, host='0.0.0.0', port=8100)
