# import uvicorn
from fastapi import FastAPI, APIRouter
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware

from upload_media_file.main import router as router_upload_media
from USER_INGESTION.main import router as router_user_ingestion
from PROCESS_DATA.main import router as router_data
from SCHEDULER.main import router as router_scheduler
from security import generate_token, validate_token


def get_application() -> FastAPI:
    application = FastAPI(
        title='IOT Fast API',
        description='IOT Fast API',
        openapi_url="/openapi.json",
        docs_url="/docs"
    )

    application.add_middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    return application


app = get_application()
router = APIRouter()
@router.post('/api_key')
def generate_token_router(topic: str):
    result = generate_token(topic)
    
    return JSONResponse(status_code=200, content=result)


app.include_router(router, prefix="/main", tags=["API_KEY"])
app.include_router(router_upload_media, prefix="/main", tags=["UPLOAD_MEDIA"])
app.include_router(router_user_ingestion, prefix="/main", tags=["USER_INGESTION"])
app.include_router(router_data, prefix="/main", tags=["DATA"])
app.include_router(router_scheduler, prefix="/main", tags=["SCHEDULER"])

#if __name__ == "__main__":
#    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=True)
