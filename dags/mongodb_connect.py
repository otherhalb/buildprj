from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pymongo

def _mongo_client_from_conn(conn_id="mongo_generic"):
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}
    is_srv = extra.get("srv", False)

    auth = ""
    if conn.login:
        auth = conn.login
        if conn.password:
            auth += f":{conn.password}"
        auth += "@"

    if is_srv:
        # Atlas SRV
        uri = f"mongodb+srv://{auth}{conn.host}/"
        client = pymongo.MongoClient(uri, tls=extra.get("tls", True), serverSelectionTimeoutMS=5000)
    else:
        host = conn.host or "localhost"
        port = conn.port or 27017
        uri = f"mongodb://{auth}{host}:{port}/"
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
    return client, (conn.schema or "admin")

def fetch_documents_from_mongo(**_):
    client, db_name = _mongo_client_from_conn("mongo_generic")
    client.admin.command("ping")
    coll = client[db_name]["customer"]
    print("✅ Ping OK")
    print("Collections:", client[db_name].list_collection_names()[:5])
    print("Count:", coll.estimated_document_count())
    print("Sample _id:", (coll.find_one({}, {"_id": 1}) or {}).get("_id"))

with DAG(
    dag_id="mongo_test_connection",
    start_date=datetime(2024, 10, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    PythonOperator(task_id="fetch_documents", python_callable=fetch_documents_from_mongo)