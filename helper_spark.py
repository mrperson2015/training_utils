"""
    This module helps to generate a SparkSession. Making it easier to start a Spark job.
    This is not intended to pe used in production.
"""
import datetime

# Standard Library Imports <- This is not required but useful to separate out imports
from pyspark.sql import SparkSession


def get_spark(application_name: str) -> SparkSession:
    """
    Boilerplate to creating a spark session

    Examples:
        get_spark(application_name="my_really_good_job_name")
    Args:
        application_name: Name of the spark job application.
        Can change this during job execution to help support logging.

    Returns:
        SparkSession
    """
    _spark = SparkSession \
        .builder \
        .appName(application_name) \
        .config("spark.driver.memory", "40g") \
        .config("spark.sql.caseSensitive", "true") \
        .master("local[*]") \
        .getOrCreate()
    _spark.sparkContext.setLogLevel("ERROR")

    return _spark


def set_application_name(application_name: str, spark_session: SparkSession) -> SparkSession:
    """
    Used to update the Spark application name during the run. This is handy for logging and troubleshooting.

    Args:
        application_name: The name of the Spark application
        spark_session: The SparkSession that you want the application name to be applied to

    Returns:
        Void
    """
    spark_session.sparkContext.appName = application_name
    return spark_session


if __name__ == '__main__':
    job_start_time = datetime.datetime.now()
    spark = get_spark(application_name="first app name")
    print(f"               Start Time ► {job_start_time}")
    print(f"Time To Get Spark Context ► {datetime.datetime.now() - job_start_time}")
    print(f"     Spark Application ID ► {spark.sparkContext.applicationId}")
    print(f"           Spark App Name ► {spark.sparkContext.appName}")
    print(f"            Spark Version ► {spark.version}")
    print(f"               Spark User ► {spark.sparkContext.sparkUser()}")
    set_application_name(application_name="second app name", spark_session=spark)
    print("Change App Name")
    print(f"           Spark App Name ► {spark.sparkContext.appName}")
