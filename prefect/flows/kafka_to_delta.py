# prefect/flows/kafka_to_delta.py
from prefect import flow, task
from kafka import KafkaConsumer
import json, os
import pandas as pd
from datetime import datetime

# Inside the worker container Kafka's INTERNAL listener is "kafka:29092"; from
# the host use "localhost:9092". docker-compose sets KAFKA_BOOTSTRAP for the worker.
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")


@task
def consume_and_process():
    """Consume data from Kafka topic"""
    consumer = KafkaConsumer(
        "data.raw",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode())
    )
    records = []
    for msg in consumer:
        records.append(msg.value)

    print(f"Consumed {len(records)} records from Kafka")
    return records

@task
def save_to_delta(records):
    """Save records to Delta Lake (parquet format)"""
    if not records:
        print("No records to save")
        return

    df = pd.DataFrame(records)
    # Giả lập Delta Lake bằng parquet (local volume)
    path = "/opt/delta-lake/raw"
    os.makedirs(path, exist_ok=True)
    df.to_parquet(f"{path}/batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
    print(f"Saved {len(df)} records to Delta Lake")

@flow(name="Kafka to Delta Pipeline")
def kafka_to_delta_flow():
    """Main flow: consume from Kafka and save to Delta Lake"""
    records = consume_and_process()
    save_to_delta(records)

if __name__ == "__main__":
    # Register a scheduled deployment against the process work pool that the
    # docker-compose worker already runs ("lab28-worker"). Run this INSIDE the
    # worker container so the code path and PREFECT_API_URL line up:
    #
    #   docker compose exec prefect-worker python /opt/prefect/flows/kafka_to_delta.py
    #
    # @flow does not accept a `schedule=` kwarg in Prefect 2.14 — the schedule
    # belongs on the deployment, not the flow.
    from prefect.deployments import Deployment
    from prefect.client.schemas.schedules import CronSchedule

    Deployment.build_from_flow(
        flow=kafka_to_delta_flow,
        name="kafka-to-delta",
        work_pool_name="lab28-worker",
        schedule=CronSchedule(cron="*/5 * * * *", timezone="UTC"),  # every 5 minutes
        path="/opt/prefect/flows",
        entrypoint="kafka_to_delta.py:kafka_to_delta_flow",
        apply=True,
    )
    print("Deployed 'kafka-to-delta' to work pool 'lab28-worker' (schedule: every 5 min)")
