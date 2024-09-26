from fastapi import FastAPI, UploadFile, File, APIRouter
from fastapi.responses import JSONResponse
import logging

from constants import VIDEO_PATH, AUDIO_PATH, ALLOWED_CONTENT_TYPES_VIDEO, ALLOWED_CONTENT_TYPES_AUDIO
from upload_media_file.helper import upload_media_file_to_minio

from fastapi import Depends
from security import validate_token


logger = logging.getLogger("main")
logging.basicConfig()
logger.setLevel(logging.INFO)

router = APIRouter()


@router.post("/upload_media/upload_video/", dependencies=[Depends(validate_token)], tags=["UPLOAD_MEDIA"])
def upload_file(file: UploadFile = File(...)):
    logger.info(f"Input filename: {file.filename}")
    if file.content_type not in ALLOWED_CONTENT_TYPES_VIDEO:
        return JSONResponse(status_code=200, content={
            'message': f"Importing data currently does not support the content type '{file.content_type}'"})
    try:
        result = upload_media_file_to_minio(file.filename, file.file, file.content_type, VIDEO_PATH)
        logger.info(f"Uploaded successfully: {result}")
        return JSONResponse(status_code=200, content=result)
    except Exception as e:
        logger.error(f"Uploading failed due to: {str(e)}")
        return JSONResponse(status_code=400, content={"message": "Uploading failed"})


@router.post("/upload_media/upload_audio/", dependencies=[Depends(validate_token)], tags=["UPLOAD_MEDIA"])
def upload_file(file: UploadFile = File(...)):
    logger.info(f"Input filename: {file.filename}")
    if file.content_type not in ALLOWED_CONTENT_TYPES_AUDIO:
        return JSONResponse(status_code=200, content={
            'message': f"Importing data currently does not support the content type '{file.content_type}'"})
    try:
        result = upload_media_file_to_minio(file.filename, file.file, file.content_type, AUDIO_PATH)
        logger.info(f"Uploaded successfully: {result}")
        return JSONResponse(status_code=200, content=result)
    except Exception as e:
        logger.error(f"Uploading failed due to: {str(e)}")
        return JSONResponse(status_code=400, content={"message": "Uploading failed"})
