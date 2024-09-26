from pydantic import BaseModel
from typing import Optional, List, Dict

class JobRequest(BaseModel):
    name: str
    function: str  # Tên hàm cần thực thi
    trigger: str  # Loại trigger
    trigger_args: Optional[Dict] = {}  # Tham số trigger
    func_kwargs: Optional[Dict] = {}


class LivySession(BaseModel):
    job_path: str = "/opt/bitnami/spark/batch-raw-to-report.py"
    livy_url: str = "http://livy:8998"
    executor_memory: str = "1G"
    livy_session_name: str = "Minh test session"
    args: List[str] = ['--bucket-name', 'cdp']


