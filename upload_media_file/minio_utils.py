from datetime import datetime, timedelta
from minio import Minio
from minio.helpers import ObjectWriteResult
from minio.error import S3Error
import logging
import time
from typing import BinaryIO

from constants import MINIO_URL, SECRET_KEY, ACCESS_KEY, MAX_RETRIES

logger = logging.getLogger("minio_utils")
logging.basicConfig()
logger.setLevel(logging.INFO)


class MinioUtil():
    __instance = None

    @staticmethod
    def get_instance():
        """ Static access method. """
        if not MinioUtil.__instance:
            MinioUtil.__instance = MinioUtil()
        return MinioUtil.__instance

    def __init__(self):

        self.minio_url = MINIO_URL
        self.access_key = ACCESS_KEY
        self.secret_key = SECRET_KEY
        self.client = Minio(
            self.minio_url,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False,
        )

    def presigned_get_object(self, bucket_name, object_name):
        # Request URL expired after 7 days
        url = self.client.presigned_get_object(
            bucket_name=bucket_name,
            object_name=object_name,
            expires=timedelta(days=7)
        )
        return url

    def presigned_put_object(self, bucket_name, object_name):
        url = self.client.presigned_put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            expires=timedelta(minutes=45)
        )
        return url

    def check_file_name_exists(self, bucket_name, file_name):
        try:
            self.client.stat_object(bucket_name=bucket_name, object_name=file_name)
            return True
        except Exception as e:
            logger.e(f'[x] Exception: {e}')
            return False

    def put_object(self, bucket_name, object_name, file_data: BinaryIO):
        retry_count = 0
        while True:
            try:
                result: ObjectWriteResult = self.client.put_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    data=file_data,
                    length=-1,
                    part_size=10 * 1024 * 1024
                )
                logger.info(f"Uploaded object '{object_name}' to bucket '{bucket_name}' successfully")
                return result
            except S3Error as s3error:
                if retry_count < MAX_RETRIES:
                    delay = 2
                    logger.error(f"Error uploading object '{object_name}'. Retrying in {delay} seconds...")
                    time.sleep(delay)
                    retry_count += 1
                else:
                    logger.error(f"Failed to upload object '{object_name}' after {MAX_RETRIES} retries.")
                    raise s3error

    def fput_object(self, bucket_name, object_name, local_file_path):
        retry_count = 0
        while True:
            try:
                result: ObjectWriteResult = self.client.fput_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    file_path=local_file_path
                )
                logger.info(f"Uploaded object '{object_name}' to bucket '{bucket_name}' successfully")
                return result
            except S3Error as s3error:
                if retry_count < MAX_RETRIES:
                    delay = 2
                    logger.error(f"Error uploading object '{object_name}'. Retrying in {delay} seconds...")
                    time.sleep(delay)
                    retry_count += 1
                else:
                    logger.error(f"Failed to upload object '{object_name}' after {MAX_RETRIES} retries.")
                    raise s3error

    def delete_object(self, bucket_name, file_name):
        try:
            self.client.remove_object(bucket_name, file_name)
            result = {
                'bucket_name': bucket_name,
                'file_name': file_name,
                'message': f"'{file_name}' was removed successfully"
            }
            return result
        except Exception as e:
            raise Exception(e)