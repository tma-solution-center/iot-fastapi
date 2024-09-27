# Config connection constant
TRINO_CONNECTION_STRING = "trino://admin@trino.trino.svc.cluster.local"
# TRINO_CONNECTION_STRING = "trino://admin@11.11.7.162:30763"
TEMP_CATALOG = 'lakehouse'
TEMP_SCHEMA = 'temp'
DEFAULT_CATALOG = 'lakehouse'
DEFAULT_SCHEMA = 'cdp'
HIVE_CATALOG = 'hive'

BUCKET_NAME = 'upload'
VIDEO_PATH = 'video'
AUDIO_PATH = 'audio'

ALLOWED_CONTENT_TYPES_VIDEO = ['video/mp4', 'video/x-msvideo', 'video/quicktime', 'video/x-matroska',
                               'video/webm', 'video/x-flv', 'video/x-ms-wmv', 'video/mpeg', 'video/3gpp']

ALLOWED_CONTENT_TYPES_AUDIO = [
    'audio/mpeg', 'audio/wav', 'audio/ogg', 'audio/flac',
    'audio/aac', 'audio/mp4', 'audio/webm', 'audio/midi',
    'audio/amr', 'audio/aiff'
]

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

# Minio Configuration
MINIO_SERVICE_NAME = 'minio'
NAMESPACE = 'minio'
MINIO_URL = f'minio.minio.svc.cluster.local:9000'
ACCESS_KEY = 'R2mXxc9KIqVrtZfV4tYF'
SECRET_KEY = 'TfVdwcIxBQc2GFegzvbzyn6VAXYSro4PNwHtnTcG'

# MINIO_URL = f'11.11.7.162:30666'
# ACCESS_KEY = 'JSYxxtSamW2C74QyELXp'
# SECRET_KEY = 'wgg1pbALe3nybZ4CabvZlOP6fVgc1NaliHyOd12s'

MAX_RETRIES = 3

# IOT
IOT_BUCKET_NAME = 'iot-demo'
IOT_SCHEMA = 'iotdemo'
