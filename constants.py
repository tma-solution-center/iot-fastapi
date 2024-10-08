TEMP_CATALOG = 'lakehouse'
TEMP_SCHEMA = 'temp'
DEFAULT_CATALOG = 'lakehouse'
DEFAULT_SCHEMA = 'cdp'
HIVE_CATALOG = 'hive'

BUCKET_NAME_UPLOAD_MEDIA = 'upload'
VIDEO_PATH = 'video'
AUDIO_PATH = 'audio'

ALLOWED_CONTENT_TYPES_VIDEO = ['video/mp4', 'application/mp4', 'video/x-msvideo', 'video/avi',
                               'video/quicktime', 'video/x-quicktime', 'video/x-matroska', 'video/webm',
                               'video/x-flv', 'video/x-ms-wmv', 'video/mpeg', 'video/3gpp',
                               'video/avi', 'video/ogg', 'video/x-ms-asf', 'video/webm']

ALLOWED_CONTENT_TYPES_AUDIO = ['audio/mpeg', 'audio/mp3', 'audio/mpeg3', 'audio/x-mpeg-3', 'audio/wav',
                               'audio/x-wav', 'audio/ogg', 'audio/flac', 'audio/aac',
                               'audio/mp4', 'audio/webm', 'audio/midi', 'audio/amr', 'audio/aiff']

TRINO_DATA_TYPE_MAPPING = {
    "int": "INTEGER",
    "bigint": "BIGINT",
    "float": "DOUBLE",
    "decimal": "DECIMAL",
    "string": "VARCHAR",
    "date": "DATE",
    "time": "TIME",
    "datetime": "TIMESTAMP",
    "bool": "BOOLEAN"
}

# IOT
IOT_BUCKET_NAME = 'iot-topics'
IOT_SCHEMA = 'iot_topics'
YEAR_STR = "year"
MONTH_STR = "month"
DAY_STR = "day"
HOUR_STR = "hour"
