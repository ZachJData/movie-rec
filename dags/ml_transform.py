from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="ml_transform",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "transform"],
    template_searchpath=["/opt/airflow/sql"],
) as dag:

    transform = PostgresOperator(
        task_id="transform_staging_to_prod",
        postgres_conn_id="postgres_default",
        sql="transform_staging.sql",
    )

    check_movie_count = PostgresOperator(
        task_id="check_movie_count",
        postgres_conn_id="postgres_default",
        sql="""
        DO $$
        BEGIN
          IF (SELECT COUNT(*) FROM movies) <> (SELECT COUNT(*) FROM movies_stage) THEN
            RAISE EXCEPTION '🛑 movie count mismatch: got % vs %',
              (SELECT COUNT(*) FROM movies), (SELECT COUNT(*) FROM movies_stage);
          END IF;
        END $$;
        """,
    )

    check_ratings_count = PostgresOperator(
        task_id="check_ratings_count",
        postgres_conn_id="postgres_default",
        sql="""
        DO $$
        BEGIN
          IF (SELECT COUNT(*) FROM ratings) <> (SELECT COUNT(*) FROM ratings_stage) THEN
            RAISE EXCEPTION '🛑 ratings count mismatch: got % vs %',
              (SELECT COUNT(*) FROM ratings), (SELECT COUNT(*) FROM ratings_stage);
          END IF;
        END $$;
        """,
    )

    check_tags_count = PostgresOperator(
        task_id="check_tags_count",
        postgres_conn_id="postgres_default",
        sql="""
        DO $$
        BEGIN
          IF (SELECT COUNT(*) FROM tags) <> (SELECT COUNT(*) FROM tags_stage) THEN
            RAISE EXCEPTION '🛑 tags count mismatch: got % vs %',
              (SELECT COUNT(*) FROM tags), (SELECT COUNT(*) FROM tags_stage);
          END IF;
        END $$;
        """,
    )

    # Chain them so checks run only after transform
    transform >> [check_movie_count, check_ratings_count, check_tags_count]

