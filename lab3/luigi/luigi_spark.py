import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round as spark_round

def main(input_file, output_dir):
    spark = SparkSession.builder \
        .appName("Lab3-Spark") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.csv(
        input_file,
        header=True,
        inferSchema=False,
        multiLine=True,
        escape='"',
        quote='"',
        encoding='utf-8',
        mode='PERMISSIVE'
    )

    df = df.select("app_id", "app_name", "review_id", "votes_helpful") \
        .withColumn("votes_helpful_int", col("votes_helpful").try_cast("integer"))

    result = df.groupBy("app_id", "app_name") \
        .agg(
            count("*").alias("total_reviews"),
            count(when(col("votes_helpful_int") >= 3, True)).alias("helpful_reviews_count")
        )

    result = result.withColumn(
        "helpful_percentage",
        spark_round((col("helpful_reviews_count") / col("total_reviews")) * 100, 2)
    )

    final_result = result.select(
        "app_id",
        "app_name",
        "helpful_reviews_count",
        "helpful_percentage"
    ).orderBy(col("helpful_reviews_count").desc())


    out_dir = Path(output_dir)
    out_dir.parent.mkdir(parents=True, exist_ok=True)

    (
        result
        .coalesce(1)              
        .write
        .option("header", "true")
        .mode("overwrite")
        .csv(str(out_dir))
    )

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Использование: python pyspark.py <input_csv> <output_dir>")
        sys.exit(0)

    input = sys.argv[1]
    output = sys.argv[2]
    main(input, output)