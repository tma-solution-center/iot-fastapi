from datetime import datetime, timedelta
import base64
import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer
from pydantic import ValidationError

reusable_oauth2 = HTTPBearer(
    scheme_name='Authorization'
)

def get_params(filename='parameters.pem'):
    with open(filename, 'r') as pem_file:
        base64_pem = pem_file.read().strip()

    decoded_bytes = base64.b64decode(base64_pem)
    decoded_pem = decoded_bytes.decode('utf-8')

    # Extract the parameter values
    parameter_lines = decoded_pem.split('\n')
    parameters = {}
    for line in parameter_lines:
        key, value = line.split('=')
        parameters[key.strip()] = value.strip()

    # Access the parameter values

    return parameters['SECURITY_ALGORITHM'], parameters['SECRET_KEY'] 

SECURITY_ALGORITHM, SECRET_KEY = get_params()

def generate_token(topic: str):
    # expire = datetime.utcnow() + timedelta(
    #     seconds = 60 # Expired after 3 hours
    # )
    to_encode = {
        # "exp": expire, 
        "username": "admin",
        "topic": topic,
    }
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=SECURITY_ALGORITHM)
    return {'api_key': [encoded_jwt]}

def validate_token(http_authorization_credentials=Depends(reusable_oauth2)) -> str:
    try:
        payload = jwt.decode(http_authorization_credentials.credentials, SECRET_KEY, algorithms=[SECURITY_ALGORITHM])
        if payload.get('username') != 'admin':
            raise HTTPException(status_code=403, detail="Could not validate credentials")
        return {
            "username": payload.get('username'),
            "topic": payload.get('topic')
        } 
    except(jwt.PyJWTError, ValidationError):
        raise HTTPException(
            status_code=403,
            detail=f"Could not validate credentials",
        )