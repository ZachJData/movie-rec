import argparse, os, tempfile
import psycopg2
from minio import Minio

def load_csv_to_stage(client, conn, bucket, prefix, filename, table):
    # create a temp filepath, then immediately close the handle so Windows won't lock it
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=filename)
    tmp_path = tmp.name
    tmp.close()

    # now download into that file
    client.fget_object(bucket, f"{prefix}/{filename}", tmp_path)

    # bulk-copy into Postgres
    with conn.cursor() as cur, open(tmp_path, "r", encoding="utf-8") as f:
        cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV HEADER", f)

    # remove the temp file
    os.remove(tmp_path)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date partition (YYYY-MM-DD)")
    parser.add_argument("--pg-host",   default="localhost")
    parser.add_argument("--pg-port",   default=5432, type=int)
    parser.add_argument("--pg-db",     default="movie_rec")
    parser.add_argument("--pg-user",   default="airflow")
    parser.add_argument("--pg-pass",   default="airflow")
    parser.add_argument("--minio-endpoint", default="localhost:9000")
    parser.add_argument("--minio-access",   default="minioadmin")
    parser.add_argument("--minio-secret",   default="minioadmin")
    parser.add_argument("--bucket",         default="raw")
    args = parser.parse_args()

    # set up clients
    client = Minio(
        "minio:9000",  # ← point at the compose service
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    conn = psycopg2.connect(
        host=args.pg_host, port=args.pg_port,
        dbname=args.pg_db, user=args.pg_user, password=args.pg_pass
    )
    try:
        for fname, table in [
            ("ratings.csv", "ratings_stage"),
            ("movies.csv",  "movies_stage"),
            ("tags.csv",   "tags_stage"),
        ]:
            print(f"→ Loading {fname} into {table}")
            load_csv_to_stage(client, conn, args.bucket, args.date, fname, table)
        conn.commit()
        print("✅ All staging tables loaded")
    except Exception as e:
        conn.rollback()
        print("❌ Error during load:", e)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
