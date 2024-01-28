import pytest
from pyspark.sql import SparkSession
import os

@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder \
        .appName("pytest_pyspark") \
        .enableHiveSupport() \
        .getOrCreate()        
    yield spark_session  # This provides the session to the test functions
    spark_session.stop()  # This stops the session after all tests are done

class TestAirlines:

    def test_passenger_ids(self,spark):
        """
        Passenger IDs should not be NULL

        Args:
            spark (SparkSession): An existing Spark session to be used for reading the CSV file.
        """
        input_df = spark \
                   .read \
                   .option("header",True) \
                   .option("inferSchema",True) \
                   .csv(os.getcwd() + "/tests/data/input/passenger_dataset.csv")
        assert input_df.select("passenger_id").filter("passenger_id is null").count() == 0

    def test_airline_passenger_distinct(self,spark):
        """
        One passenger has a unique record for every airline route

        Args:
            spark (SparkSession): An existing Spark session to be used for reading the CSV file.       
        """
        input_df = spark \
                   .read \
                   .option("header",True) \
                   .option("inferSchema",True) \
                   .csv(os.getcwd() + "/tests/data/input/passenger_dataset.csv")
        assert input_df \
        .select("passenger_id","airline") \
        .filter("passenger_id is not null") \
        .groupBy("airline","passenger_id") \
        .count() \
        .filter("count > 1") \
        .count() == 0        

    def test_city_route(self,spark):
        """
        Every origin city must have same destination city

        Args:
            spark (SparkSession): An existing Spark session to be used for reading the CSV file.       
        """
        route_map = {
            "Mumbai": "Dubai",
            "Singapore": "Tokyo",
            "Berlin": "New York"
        }
        input_df = spark \
                   .read \
                   .option("header",True) \
                   .option("inferSchema",True) \
                   .csv(os.getcwd() + "/tests/data/input/passenger_dataset.csv")        
        for origin, dest in route_map.items():
            check_origin = input_df \
            .select("origin_city","destination_city") \
            .filter(f"origin_city == '{origin}'") \
            .select("destination_city") \
            .distinct() \
            .collect()[0][0] 
            assert check_origin == dest

    def test_airline_a_count(self,spark):
        """
        List of 300 passengers matching for Airline A

        Args:
            spark (SparkSession): An existing Spark session to be used for reading the CSV file.       
        """     
        input_df = (
            spark
            .read
            .option("header",True)
            .option("inferSchema",True)
            .csv(os.getcwd() + "/tests/data/input/passenger_dataset.csv")
            .select("airline", "origin_city", "destination_city", "passenger_name")
            .filter("airline == 'Airline A'")
            .orderBy("passenger_name")
        )
        expected_df = (
            spark
            .read
            .option("header",True)
            .option("inferSchema",True)
            .csv(os.getcwd() + "/tests/data/expected/airline_a_passengers.csv")
            .orderBy("passenger_name")
        )
        diff_df = input_df.exceptAll(expected_df)
        assert diff_df.count() == 0            
