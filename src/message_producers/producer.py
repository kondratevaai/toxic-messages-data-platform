import json 
from multiprocessing.dummy import connection
import os 
# client to work with RabbitMQ: 
import pika


class ToxMEssagesProducer:
    def __init__(self, config_path: str):
        # load config from json file
        self.config = self.load_config(config_path)

    def load_source_config(self, config_path: str) -> dict:
        with open(config_path, 'r') as f:
            return json.load(f)

    def connect(self):
        """Connects to the RabbitMQ broker and sets up the channel and queue."""
        
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()