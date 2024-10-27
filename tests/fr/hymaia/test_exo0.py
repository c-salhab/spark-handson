from src.fr.hymaia.exo2.aggregate import aggregate_population_by_department
from src.fr.hymaia.exo2.spark_clean_job import filter_adult_clients, zip_join, add_department, \
    filter_senior_perpignan_clients
from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo1.main import wordcount
from pyspark.sql import Row


class TestMain(unittest.TestCase):
    def test_wordcount(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(text='bonjour je suis un test unitaire'),
                Row(text='bonjour suis test')
            ]
        )

        expected = spark.createDataFrame(
            [
                Row(word='bonjour', count=2),
                Row(word='je', count=1),
                Row(word='suis', count=2),
                Row(word='un', count=1),
                Row(word='test', count=2),
                Row(word='unitaire', count=1),
            ]
        )

        actual = wordcount(input, 'text')
        self.assertCountEqual(actual.collect(), expected.collect())

    def test_filter_adult_clients(self):
        # GIVEN
        input_df = spark.createDataFrame([
            Row(name='charbel', age=24, zip="75200"),
            Row(name='sansa', age=23, zip="77000"),
            Row(name='marc', age=15, zip="94380")
        ])

        expected_df = spark.createDataFrame([
            Row(name='charbel', age=24, zip="75200"),
            Row(name='sansa', age=23, zip="77000"),
        ])

        actual_df = filter_adult_clients(input_df)
        self.assertEqual(actual_df.collect(), expected_df.collect())

    def test_zip_join(self):
        # GIVEN
        clients_df = spark.createDataFrame([
            Row(name='charbel', age=24, zip="75001"),
            Row(name='sansa', age=23, zip="77000"),
        ])

        cities_df = spark.createDataFrame([
            Row(zip="75001", city="Paris"),
            Row(zip="77000", city="Melun"),
            Row(zip="94380", city="Bonneuil")
        ])

        expected_df = spark.createDataFrame([
            Row(name='charbel', age=24, zip="75001", city="Paris"),
            Row(name='sansa', age=23, zip="77000", city="Melun"),
        ])

        actual_df = zip_join(clients_df, cities_df)

        actual_dicts = [row.asDict() for row in actual_df.collect()]
        expected_dicts = [row.asDict() for row in expected_df.collect()]

        self.assertEqual(sorted(actual_dicts, key=lambda x: x['name']),
                         sorted(expected_dicts, key=lambda x: x['name']))

    def test_add_department(self):
        # GIVEN
        input_df = spark.createDataFrame([
            Row(name='charbel', zip="75001"),
            Row(name='jean', zip="20090"),
            Row(name='pierre', zip="20200"),
            Row(name='sansa', zip="77000"),
        ])

        expected_df = spark.createDataFrame([
            Row(name='charbel', zip="75001", department="75"),
            Row(name='jean', zip="20090", department="2A"),
            Row(name='pierre', zip="20200", department="2B"),
            Row(name='sansa', zip="77000", department="77"),
        ])

        actual_df = add_department(input_df)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_df.collect()))

    def test_aggregate_population_custom_column(self):
        # GIVEN
        input_df = spark.createDataFrame([
            Row(name='charbel', dept="75"),
            Row(name='marie', dept="75"),
            Row(name='pierre', dept="92"),
        ])

        expected_df = spark.createDataFrame([
            Row(dept="75", nb_of_people=2),
            Row(dept="92", nb_of_people=1)
        ])

        actual_df = aggregate_population_by_department(input_df, department_column_name="dept")

        actual_result = [(row.dept, row.nb_of_people) for row in actual_df.collect()]
        expected_result = [(row.dept, row.nb_of_people) for row in expected_df.collect()]
        self.assertEqual(actual_result, expected_result)

    def test_filter_senior_perpignan_clients(self):
        # GIVEN
        input_df = spark.createDataFrame([
            Row(name='Alice', age=35, zip="66000"),  
            Row(name='Bob', age=25, zip="66100"),
            Row(name='Charlie', age=45, zip="75000"),  
            Row(name='David', age=32, zip="66200"),  
            Row(name='Eve', age=28, zip="66300")  
        ])

        expected_df = spark.createDataFrame([
            Row(name='Alice', age=35, zip="66000"),
            Row(name='David', age=32, zip="66200")
        ])

        actual_df = filter_senior_perpignan_clients(input_df)

        self.assertEqual(sorted(actual_df.collect()), sorted(expected_df.collect()))
