import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark_etl.py <date>")
        sys.exit(1)
    date = sys.argv[1]  # e.g. "2025-06-14"

    spark = (
        SparkSession.builder
        .appName(f"MovieLens_ETL_{date}")
        .getOrCreate()
    )

    # point Spark's S3 filesystem at MinIO:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    # this file contains your fs.s3a.* settings
    # you passed it via `--files hadoop-aws-setup.conf`
    hadoop_conf.addResource("hadoop-aws-setup.conf")

    raw_path = f"s3a://raw/{date}"

    movies_df = (spark.read
                 .option("header", True)
                 .csv(f"{raw_path}/movies.csv"))
    print("→ movies.csv rows:", movies_df.count())

    ratings_df = (spark.read
                  .option("header", True)
                  .csv(f"{raw_path}/ratings.csv"))
    print("→ ratings.csv rows:", ratings_df.count())

    # simple join
    joined = ratings_df.join(
        movies_df,
        on="movieId",
        how="inner"
    )

    # compute user mean rating
    user_mean = (
        joined.groupBy("userId")
              .agg(avg(col("rating")).alias("user_mean_rating"))
    )

    # write out parquet
    out_base = f"s3a://processed/features/{date}"
    user_mean.write.mode("overwrite").parquet(f"{out_base}/user_mean")

    spark.stop()

