import os 
import json
import pickle 
import boto3
import io
import pandas as pd 

from pathlib import Path

from core.config import MODEL_CONFIG


BASE_DIR = Path(__file__).resolve().parent

def load_config(config_path: str = MODEL_CONFIG):
    
    # path = BASE_DIR / Path(config_path)
    path = config_path
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # for key in ["available_predictors", "available_encoders", 
    # "default_predictor", "default_encoder"]:
    for key in ["predictors"]:
        if key not in config:
            raise ValueError(f"Missing key '{key}' in config file")
    
    # check remote-local correspondence: 
    is_s3 = 's3' in [obj.get("storage_type") for obj in config.get("predictors")]
    for s3_key in ["aws_endpoint_url", "aws_access_key_id", "aws_secret_access_key"]: 
        if len(s3_key)==0 and is_s3:
            raise ValueError(f"Missing S3 config key '{s3_key}' in config file")
    
    return config

def load_pickle(config: dict): 
    """Local/remote loading of pickle model weights based on config"""

    storage_type = config.get("storage_type")
    # print(config)

    if storage_type == "local":
        # TODO: fix strange paths 
        model_path = config.get("model_path")

        model_path = BASE_DIR.parent.parent / Path(model_path)

        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model file not found at: {model_path}")

        with open(model_path, "rb") as f: 
            model = pickle.load(f)

    elif storage_type == "s3": 
        reader = Boto3Reader(
            config_path=MODEL_CONFIG, 
            bucket_name=config.get("bucket_name")
        )

        # get raw bytes: 
        model = reader.get_boto3_obj(config.get("model_path"))
        # load pickle from bytes: 
        model = pickle.load(io.BytesIO(model['Body'].read()))
    else: 
        raise ValueError(f"Unsupported storage type: {storage_type}")

    return model

def load_local_pickle(model_path: str): 
    
    # TODO: fix strange paths 
    model_path = BASE_DIR.parent.parent / Path(model_path)

    # model_path = Path(model_path)
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found at: {model_path}")

    with open(model_path, "rb") as f:
        model = pickle.load(f)

    return model

def load_encoder(config): 

    storage_type = config.get("storage_type")
    
    if storage_type == "local":
        encoder_path = config.get("encoder_path")
        # TODO: fix strange paths 
        encoder_path = BASE_DIR.parent.parent /Path(encoder_path)
        if not encoder_path.exists():
            raise FileNotFoundError(f"Model file not found at: {encoder_path}")
        
        with open(encoder_path, "rb") as f:
            encoder = pickle.load(f)
    elif storage_type == "s3":
        reader = Boto3Reader(
            config_path=MODEL_CONFIG, 
            bucket_name=config.get("bucket_name")
        )

        # get raw bytes: 
        encoder = reader.get_boto3_obj(config.get("encoder_path"))
        # load pickle from bytes: 
        encoder = pickle.load(io.BytesIO(encoder['Body'].read()))

    return encoder


def load_local_encoder(encoder_path: str): 

    # TODO: fix strange paths 
    encoder_path = BASE_DIR.parent.parent /Path(encoder_path)
    if not encoder_path.exists():
        raise FileNotFoundError(f"Model file not found at: {encoder_path}")
    
    with open(encoder_path, "rb") as f:
        encoder = pickle.load(f)

    return encoder

def load_loc_enc_json(path: str) -> dict: 
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found at: {path}")

    with open(path, "r", encoding="utf-8") as f:
        config = json.load(f)

    return config

def load_s3_enc_json(config: dict, path: str) -> dict:
    reader = Boto3Reader(
        config_path=MODEL_CONFIG, 
        bucket_name=config.get("bucket_name")
    )

    # get raw bytes: 
    enc_json = reader.get_boto3_obj(path)
    # load json from bytes: 
    enc_json = json.loads(enc_json['Body'].read())

    return enc_json

def load_s3_txt(config: dict, path: str) -> list:
    reader = Boto3Reader(
        config_path=MODEL_CONFIG, 
        bucket_name=config.get("bucket_name")
    )

    # get raw bytes: 
    enc_txt = reader.get_boto3_obj(path)
    # load txt from bytes: 
    enc_txt = enc_txt['Body'].read().decode('utf-8').splitlines()

    return enc_txt

class Boto3Base: 
    """Base class for boto3 client handling S3 interactions"""

    def __init__(self, config_path='src/config_s3.json'):
        self.config: dict = load_config(config_path)
    
    def get_client(self):
        
        endpoint_url = self.config.get('aws_endpoint_url')
        access_key, secret_key = self.config.get('aws_access_key_id'), \
                                self.config.get('aws_secret_access_key')
        
        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        self.s3_client = s3_client

        return s3_client
    
class Boto3Reader(Boto3Base): 
    def __init__(self, config_path='src/config_s3.json', 
                 bucket_name='toxic-messages-bucket-1'):
        super().__init__(config_path)

        self.get_client()
        self.bucket_name = bucket_name


    def get_boto3_obj(self, object_path: str) -> bytes: 
        """:param str object_path: full object name without bucket"""

        # print(object_path)
        # print(self.bucket_name)
        if self.bucket_name in object_path: 
            object_path = object_path.replace(f"{self.bucket_name}/", "")
        
        return self.s3_client.get_object(Bucket=self.bucket_name, Key=object_path)
    
    def get_boto3_csv(self, object_path: str) -> pd.DataFrame: 
        obj = self.get_boto3_obj(object_path)['Body'].read()
        df = pd.read_csv(io.BytesIO(obj), encoding='utf8', index_col=0)

        return df
    
    def get_boto3_json(self, object_path: str): 
        obj = self.get_boto3_obj(object_path)['Body'].read()
        return json.loads(obj)