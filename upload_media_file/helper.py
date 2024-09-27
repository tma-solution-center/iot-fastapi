from datetime import datetime, timezone
from typing import BinaryIO

import unicodedata

from constants import BUCKET_NAME_UPLOAD_MEDIA
from common.MinioUtils import MinioUtils


def upload_media_file_to_minio(file_name: str, file_data: BinaryIO, content_type, sub_path):
    file_name = remove_accents_and_replace(file_name)
    utc_now = datetime.now(timezone.utc)
    datetime_suffix = utc_now.strftime("%Y%m%d%H%M%S")
    datatime_as_path = utc_now.strftime("%Y/%m/%d")
    object_name = f"/{sub_path}/{datatime_as_path}/{datetime_suffix}_{file_name}"

    minio_util = MinioUtils().get_instance()
    result = minio_util.put_object(BUCKET_NAME_UPLOAD_MEDIA, object_name, file_data)
    return {
        "bucket_name": result.bucket_name,
        "object_name": result.object_name,
        "version_id": result.version_id,
        "last_modified": result.last_modified
    }


def remove_accents_and_replace(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    result_str = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

    result_str = result_str.replace('ă', 'a') \
        .replace('â', 'a').replace('đ', 'd') \
        .replace('ê', 'e').replace('ô', 'o') \
        .replace('ơ', 'o').replace('ư', 'u').replace(' ', '_')

    return result_str
