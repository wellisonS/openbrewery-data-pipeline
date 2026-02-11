from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, current_date

spark = SparkSession.builder.getOrCreate()


def transform_openbrewery():
    df = spark.table("bronze.openbrewery_raw")

    df_clean = (
        df
        .select(
            col("id"),
            col("name"),
            lower(col("brewery_type")).alias("brewery_type"),
            col("city"),
            col("state"),
            col("country"),
            col("latitude").cast("double"),
            col("longitude").cast("double")
        )
        .dropDuplicates(["id"])
        .filter(col("id").isNotNull())
        .withColumn("ingestion_date", current_date())
    )

    return df_clean


def write_silver(df):
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

    (
        df.write
        .mode("overwrite")
        .partitionBy("country", "state")
        .saveAsTable("silver.openbrewery")
    )


def run():
    df = transform_openbrewery()
    write_silver(df)


if __name__ == "__main__":
    run()
