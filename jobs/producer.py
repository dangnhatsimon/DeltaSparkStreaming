from confluent_kafka import Producer, KafkaError, Message
import json
import time
import logging
from typing import Optional, Union, List, Tuple, Any, Dict
from dotenv import load_dotenv
import os
from typing import Optional, Union, List, Tuple, Any, Literal
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr

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

    def delivery_report(self, err: KafkaError, msg: Message):
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


class KafkaProducer:
    def __init__(self, spark: SparkSession):
        self.BOOTSTRAP_SERVERS = os.getenv("bootstrap.servers")
        self.SECURITY_PROTOCOL = os.getenv("security.protocol")
        self.SASL_MECHANISM = os.getenv("sasl.mechanism")
        self.SASL_USERNAME = os.getenv("sasl.username")
        self.SASL_PASSWORD = os.getenv("sasl.password")
        self.JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
        self.SASL_JAAS = f"""{self.JAAS_MODULE} required username='{self.SASL_USERNAME}' password='{self.SASL_PASSWORD}';"""
        self.spark = spark

    def read_invoices(self, format: str, schema: str, path:str, condition: str) -> DataFrame:
        return (self.spark.readStream
                .format(format)
                .schema(schema)
                .load(path)
                .where(condition))

    def get_message(self, df: DataFrame, key: str):
        return df.selectExpr(f"{key} AS key, to_json(struct(*)) AS value")

    def send_message(self, df: DataFrame, topic: str, checkpoint_location: str, output_mode: Literal["update", "complete", "append"]):
        return (df.writeStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVERS)
                .option("kafka.security.protocol", self.SECURITY_PROTOCOL)
                .option("kafka.sasl.mechanism", self.SASL_MECHANISM)
                .option("kafka.sasl.jaas.config", self.SASL_JAAS)
                .option("topic", topic)
                .option("checkpointLocation", checkpoint_location)
                .outputMode(output_mode)
                .start())


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

    # path = "/opt/spark/datasets/invoices/invoices-1.json"
    # invoice_producer = InvoiceProducer(topic, conf)
    # invoice_producer.produce_invoices(
    #     path=path,
    #     counts=10
    # )
    # invoice_producer.flush(10)

    schema = """
            InvoiceNumber string,
            CreatedTime bigint,
            StoreID string,
            PosID string,
            CashierID string,
            CustomerType string,
            CustomerCardNo string,
            TotalAmount double,
            NumberOfItems bigint,
            PaymentMethod string,
            TaxableAmount double,
            CGST double,
            SGST double,
            CESS double,
            DeliveryType string,
            DeliveryAddress struct<
                AddressLine string,
                City string,
                ContactNumber string,
                PinCode string,
                State string
            >,
            InvoiceLineItems array<
                struct<
                    ItemCode string,
                    ItemDescription string,
                    ItemPrice double,
                    ItemQty bigint,
                    TotalValue double
                >
            >
        """
    path = "/opt/spark/datasets/invoices/*.json"
    condition = "StoreID == 'STR7188'"

    spark = (
        SparkSession.builder
        .appName("InvoicesKafka")
        .enableHiveSupport()
        .getOrCreate()
    )

    kafka_producer = KafkaProducer(spark)
    invoice_df = kafka_producer.read_invoices(
        format="json",
        schema=schema,
        path=path,
        condition=condition
    )
    kafka_df = kafka_producer.get_message(
        df=invoice_df,
        key="StoreID"
    )
    squery = kafka_producer.send_message(
        df=kafka_df,
        topic=topic,
        checkpoint_location="/opt/spark/spark-checkpoint",
        output_mode="append"
    )