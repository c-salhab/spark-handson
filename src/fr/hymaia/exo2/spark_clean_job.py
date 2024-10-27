"""
Charbel SALHAB
4IABD1
spark-hadson-hymaia
10/2024
"""

import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def filter_adult_clients(df, col_name="age"):
    return df.filter(f.col(col_name) >= 18)


def filter_senior_perpignan_clients(df, age_col="age", zip_col="zip"):
    return df.filter((f.col(age_col) > 30) & (f.substring(f.col(zip_col), 1, 2) == "66"))


def zip_join(data_frame, data_frame_city, on="zip", how="inner"):
    return data_frame.join(other=data_frame_city, on=on, how=how)


def add_department(data_frame, column="zip", department_column_name="department"):
    data_frame_department = data_frame.withColumn(department_column_name,
                                                  f.when((f.substring(f.col(column), 1, 2) == "20") & (
                                                          f.col(column) <= "20190"), "2A") \
                                                  .when((f.substring(f.col(column), 1, 2) == "20") & (
                                                          f.col(column) > "20190"), "2B") \
                                                  .otherwise(f.substring(f.col(column), 1, 2)))

    return data_frame_department


def spark_clean_job():
    spark_session = SparkSession.builder.appName("hymaia").master("local[*]").getOrCreate()
    city_zipcode_file = spark_session.read.option("header", True).csv("../../../src/resources/exo2/city_zipcode.csv")
    clients_bdd_file = spark_session.read.option("header", True).csv("../../../src/resources/exo2/clients_bdd.csv")

    # les clients majeurs > 18
    major_clients = filter_adult_clients(clients_bdd_file)

    # connaitre le nom de la ville ou habitent les clients majeurs
    major_client_city = zip_join(major_clients, city_zipcode_file)

    # ajouter une colonne "departement" -> fichier output
    major_clients_cities_department = add_department(major_client_city)

    # les clients > 30 et don le zip commence par 66
    senior_perpignan_clients = filter_senior_perpignan_clients(clients_bdd_file)

    major_clients_cities_department.write.mode("overwrite").option("header", True).parquet("../../data/exo2/clean")


if __name__ == "__main__":
    spark_clean_job()


