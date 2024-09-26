# Config connection constant
TRINO_CONNECTION_STRING = "trino://admin@trino.trino.svc.cluster.local/lakehouse"
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

# Minio Configuration
MINIO_SERVICE_NAME = 'minio'
NAMESPACE = 'minio'
MINIO_URL = f'minio.minio.svc.cluster.local:9000'
ACCESS_KEY = 'R2mXxc9KIqVrtZfV4tYF'
SECRET_KEY = 'TfVdwcIxBQc2GFegzvbzyn6VAXYSro4PNwHtnTcG'
MAX_RETRIES = 3
