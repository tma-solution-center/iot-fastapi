from pydantic import BaseModel
from typing import Optional, List


class CreateTopicRequest(BaseModel):
    name: str
    num_partitions: int
    replication_factor: int


class ConnectorConfig(BaseModel):
    name: str
    config: dict


class KafkaMessage(BaseModel):
    data: dict


class DebeziumMessage(BaseModel):
    before: Optional[dict]
    after: dict
    source: dict
    topic: str 


class Session(BaseModel):
    livy_host: str = "http://livy.livy.svc.cluster.local:8998"
    job_path: str = "/opt/bitnami/spark/batch-raw-to-report.py"
    executor_memory: str = "1G"
    num_executors: int = 1
    name: str = "Minh test Livy session"
    args: List[str] = ['--bucket-name', 'cdp', '--app-name', 'My Spark app1', '--cores-max', '1']
