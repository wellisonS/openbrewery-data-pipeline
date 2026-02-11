import requests
from pyspark.sql import SparkSession
from datetime import date

spark = SparkSession.builder.getOrCreate()

BASE_URL = "https://api.openbrewerydb.org/v1/breweries"


def fetch_breweries(page_size=200, max_pages=5):
    results = []

    for page in range(1, max_pages + 1):
        resp = requests.get(
            BASE_URL,
            params={"per_page": page_size, "page": page},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()

        if not data:
            break

        results.extend(data)

    return results


def write_bronze_layer(data):
    df = spark.createDataFrame(data)

    (
        df.write
        .mode("overwrite")
        .saveAsTable("bronze.openbrewery_raw")
    )


def run():
    data = fetch_breweries()
    write_bronze_layer(data)


if __name__ == "__main__":
    run()
