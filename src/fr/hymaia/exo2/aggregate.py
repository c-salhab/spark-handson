"""
Charbel SALHAB
4IABD1
spark-hadson-hymaia
10/2024
"""

import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def aggregate_population_by_department(data_frame, department_column_name="department"):
    return data_frame.groupBy(department_column_name).count().withColumnRenamed("count", "nb_of_people") \
        .sort(f.desc(f.col("nb_of_people")), f.col(department_column_name))


def aggregate():
    spark_session = SparkSession.builder.appName("hymaia").master("local[*]").getOrCreate()
    client_department_parquet_file = spark_session.read.parquet("../../data/exo2/clean")

    population_by_department = aggregate_population_by_department(client_department_parquet_file)
    population_by_department.write.mode("overwrite").option("header", True).parquet("../../data/exo2/aggregate")


if __name__ == "__main__":
    aggregate()
