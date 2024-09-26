from fastapi import APIRouter, HTTPException
from fastapi import Depends
from security import validate_token
from fastapi.responses import JSONResponse
from USER_INGESTION.models import CreateTopicRequest, ConnectorConfig, KafkaMessage, DebeziumMessage, Session
from confluent_kafka.admin import AdminClient, NewTopic, KafkaException
from confluent_kafka import Producer
# from confluent_kafka import KafkaException
from requests import RequestException, post, get, put, delete
from datetime import datetime
import json


router = APIRouter()


# Kafka   configuration
KAFKA_BROKER = 'kafka-controller-headless.kafka.svc.cluster.local:9092'  # Địa chỉ của Kafka broker
KAFKA_CONNECT_URL = "http://connect.kafka-connect.svc.cluster.local:8083/connectors"

# Configure Kafka producer
producer = Producer({'bootstrap.servers': KAFKA_BROKER})
# Configure Kafka admin client
admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
       Triggered by poll() or flush()."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# TOPICS        
# @router.post("/topics/", dependencies=[Depends(validate_token)], tags=["USER_INGESTION"])
# def create_topic(request: CreateTopicRequest):
#     topic = NewTopic(request.name, num_partitions=request.num_partitions, replication_factor=request.replication_factor)
#     fs = admin_client.create_topics([topic])
    
#     for topic, f in fs.items():
#         try:
#             f.result()  # The result itself is None
#             return {"status": "success", "message": f"Topic '{topic}' created successfully"}
#         except KafkaException as e:
#             raise HTTPException(status_code=500, detail=str(e))

# @router.delete("/topics/{name}", dependencies=[Depends(validate_token)], tags=["USER_INGESTION"])
# def delete_topic(name: str):
#     fs = admin_client.delete_topics([name])
    
#     for topic, f in fs.items():
#         try:
#             f.result()  # The result itself is None
#             return {"status": "success", "message": f"Topic '{topic}' deleted successfully"}
#         except KafkaException as e:
#             raise HTTPException(status_code=500, detail=str(e))

# @router.get("/topics/", dependencies=[Depends(validate_token)], tags=["USER_INGESTION"])
# def list_topics():
#     topics_metadata = admin_client.list_topics().topics
#     topic_names = list(topics_metadata.keys())
#     return {"topics": topic_names}


# # CONNECTORS
# @router.post("/connectors/", dependencies=[Depends(validate_token)], tags=["USER_INGESTION"])
# def create_connector(connector: ConnectorConfig):
#     url = KAFKA_CONNECT_URL
#     data = {
#         "name": connector.name,
#         "config": connector.config
#     }
#     try:
#         response = post(url, json=data)
#         response.raise_for_status()
#         return response.json()
#     except RequestException as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @router.get("/connectors/{name}/status", dependencies=[Depends(validate_token)], tags=["USER_INGESTION"])
# def get_connector_status(name: str):
#     url = f"{KAFKA_CONNECT_URL}/{name}/status"
#     try:
#         response = get(url)
#         response.raise_for_status()
#         return response.json()
#     except RequestException as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @router.put("/connectors/{name}/config", dependencies=[Depends(validate_token)], tags=["USER_INGESTION"])
# def update_connector_config(name: str, config: dict):
#     url = f"{KAFKA_CONNECT_URL}/{name}/config"
#     try:
#         response = put(url, json=config)
#         response.raise_for_status()
#         return response.json()
#     except RequestException as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @router.delete("/connectors/{name}", dependencies=[Depends(validate_token)], tags=["USER_INGESTION"])
# def delete_connector(name: str):
#     url = f"{KAFKA_CONNECT_URL}/{name}"
#     try:
#         response = delete(url)
#         response.raise_for_status()
#         return {"message": "Connector deleted successfully"}
#     except RequestException as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @router.get("/connectors/", dependencies=[Depends(validate_token)], tags=["USER_INGESTION"])
# def list_connectors():
#     url = KAFKA_CONNECT_URL
#     try:
#         response = get(url)
#         response.raise_for_status()
#         return response.json()
#     except RequestException as e:
#         raise HTTPException(status_code=500, detail=str(e))

# EXAMPLES    
@router.post("/produce/", dependencies=[Depends(validate_token)], tags=["USER_INGESTION"])
def produce_message(message: KafkaMessage):
    try:
        producer.produce(message.topic, key=None, value=json.dumps(message.data), callback=delivery_report)
        producer.flush()  # Wait for all messages to be delivered
    except KafkaException as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    return {"status": "success", "data": message.data, "topic": message.topic}

# @router.post("/produce/debezium/", dependencies=[Depends(validate_token)], tags=["USER_INGESTION"])
# def produce_debezium_message(message: DebeziumMessage):
#     try:
#         producer.produce(
#             message.topic,
#             key=None,
#             value=json.dumps({
#                 "before": message.before,
#                 "after": message.after,
#                 "source": message.source
#             }),
#             callback=delivery_report
#         )
#         producer.flush()  # Wait for all messages to be delivered
#     except KafkaException as e:
#         raise HTTPException(status_code=500, detail=str(e))
    
#     return {"status": "success", "data": message.dict()}


# @router.post("/livy/trigger/", dependencies=[Depends(validate_token)], tags=["USER_INGESTION"])
# def livy_trigger_job(request: Session):
#     body = {
#         "file": f"local://{request.job_path}",
#         "name": request.name,
#         "executorMemory": request.executor_memory,
#         "args": request.args
#     }
    
#     headers = dict()
#     # headers["accept"] = "application/json"
#     headers["Content-Type"] = "application/json"
#     body = json.dumps(body) # covert ' -> "

#     try:
#         response = post(f"{request.livy_host}/batches", data=body, headers=headers)
#         return JSONResponse(status_code=response.status_code, content={"message": response.json()})
#     except Exception as e:
#         return JSONResponse(content={"message": str(e)})


