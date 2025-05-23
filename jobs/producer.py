from confluent_kafka import Producer
import json
import time
import logging
from typing import Optional, Union, List, Tuple, Any, Dict
from dotenv import load_dotenv
import os


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)

load_dotenv(dotenv_path="/opt/spark/spark-config/.env")


class InvoiceProducer:
    def __init__(self, topic: str, conf: Dict[str, str]):
        self.topic = topic
        self.conf = conf
        self.producer = Producer(self.conf)

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            key = msg.key().decode("utf-8")
            invoice_id = json.loads(msg.value().decode("utf-8"))["InvoiceNumber"]
            print(f"Message key: {key}, value: {invoice_id} delivered to {msg.topic()} [{msg.partition()}]")

    def produce_invoices(self, path: str, counts: int):
        counter = 0
        with open(path, "+rb") as invoices:
            for invoice in invoices:
                invoice = json.loads(invoice)
                store_id = invoice["StoreId"]
                self.producer.produce(topic=self.topic, key=store_id, value=invoice, callback=self.delivery_report)
                self.producer.poll(1)
                counter += 1
                if counter == counts:
                    break

    def flush(self, timeout: float):
        self.producer.flush(timeout)


if __name__ == "__main__":
    topic = "invoices"
    conf = {
        "bootstrap.servers": os.getenv("bootstrap.servers"),
        "security.protocol": os.getenv("security.protocol"),
        "sasl.mechanism": os.getenv("sasl.mechanism"),
        "sasl.username": os.getenv("sasl.username"),
        "sasl.password": os.getenv("sasl.password"),
        "client.id": os.getenv("client.id")
    }
    path = "/opt/spark/datasets/invoices/invoices-1.json"

    invoice_producer = InvoiceProducer(topic, conf)
    invoice_producer.produce_invoices(
        path=path,
        counts=10
    )
    invoice_producer.flush(10)