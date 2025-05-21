from confluent_kafka import Producer
import json
import time
import logging
from typing import Optional, Union, List, Tuple, Any, Dict
from config import configuration


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)


class InvoiceProducer:
    def __init__(self, topic: str, conf: Dict[str, str]):
        self.topic = topic
        self.conf = conf

    def delivery_callback(self, err, msg):

    def produce_from_file(self, producer: Producer, path: str):
        with open(path) as invoices:
            for invoice in invoices:
                invoice = json.loads(invoice)
                store_id = invoice["StoreId"]
                producer.produce(topic=self.topic, key=store_id, value=invoice, callback=self.delivery_callback)




topic = "invoices"
conf = {
    "bootstrap.servers": configuration.get("bootstrap.servers"),
    "security.protocol": configuration.get("security.protocol"),
    "sasl.mechanism": configuration.get("sasl.mechanism"),
    "sasl.username": configuration.get("sasl.username"),
    "sasl.password": configuration.get("sasl.password"),
    "client.id": configuration.get("client.id")
}
path = "/opt/spark/datasets/invoices/invoices-1.json"