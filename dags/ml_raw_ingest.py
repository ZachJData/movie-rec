from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.http.hooks.http import HttpHook
from minio import Minio
import zipfile, os

DEFAULT_ARGS = {"owner": "you", "retries": 1}

def check_and_download(ti, **ctx):
    hook = HttpHook(http_conn_id="movielens_api", method="HEAD")
    resp = hook.run("datasets/movielens/ml-latest-small.zip")
    remote_etag = resp.headers.get("ETag")
    prev = ti.xcom_pull(task_ids="get_previous_etag")
    if remote_etag == prev:
        raise AirflowSkipException("No new MovieLens data")
    get = HttpHook(http_conn_id="movielens_api", method="GET")
    data = get.run("datasets/movielens/ml-latest-small.zip").content
    with open("/tmp/ml.zip", "wb") as f:
        f.write(data)
    ti.xcom_push(key="new_etag", value=remote_etag)

def unpack_and_upload(**ctx):
    client = Minio(
      "host.docker.internal:9000",
      access_key="minioadmin",
      secret_key="minioadmin",
      secure=False
    )
    date = ctx["ds"]  # e.g. "2025-06-11"
    with zipfile.ZipFile("/tmp/ml.zip", "r") as z:
        for member in z.namelist():
            if member.endswith(".csv"):
                local = z.extract(member, "/tmp")
                dest = f"{date}/{os.path.basename(member)}"
                client.fput_object("raw", dest, local)

with DAG(
    dag_id="ml_raw_ingest",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ingest"],
) as dag:

    get_previous_etag = PythonOperator(
        task_id="get_previous_etag",
        python_callable=lambda **ctx: None
    )

    download = PythonOperator(
        task_id="check_and_download",
        python_callable=check_and_download
    )

    upload = PythonOperator(
        task_id="unpack_and_upload",
        python_callable=unpack_and_upload
    )

    get_previous_etag >> download >> upload
