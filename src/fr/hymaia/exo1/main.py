import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    # print("Hello world!")

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("wordcount") \
        .getOrCreate()

    try:

        csv_df = spark.read \
            .option("header", True) \
            .csv("/home/csalhab/projetspark/spark-handson-main/src/resources/exo1/data.csv")

        output_path = "/home/csalhab/projetspark/spark-handson-main/src/fr/data/exo1/output"

        result_df = wordcount(csv_df, "text") \
        .write \
        .mode("overwrite") \
        .partitionBy("count") \
        .parquet(output_path)

        # print("ok ca marche")

        # parquet_df = spark.read.parquet(output_path)
        # parquet_df.show(truncate=False)
        
    finally:
        spark.stop()
def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()

if __name__ == "__main__":
    main()




