"""
    This is module generates random data in a Spark dataframe. This leverages the data generator module.
    It is built to be used in generating sample data in our PySpark jobs during development.
"""

# Standard Library Imports <- This is not required but useful to separate out imports
import decimal
import os

from pyspark.sql.functions import udf
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from helper_spark import *
import helper_data_generator as dg
from helper_printer import print_header


@udf(returnType=T.StringType())
def get_record_action():
    """
    Returns a random string of possible record changes

    Examples:
        delete\n
        update\n
        insert\n
        no_change

    Returns:
        str
    """
    return dg.get_record_action()


@udf(returnType=T.IntegerType())
def random_int():
    """
    Returns a random integer

    Examples:
        6753

    Returns:
        int
    """
    return dg.random_int()


@udf(returnType=T.FloatType())
def random_float():
    """
    Returns a random float

    Examples:
        39.4500250260523
        -1056327468048.49

    Returns:
        float
    """
    return dg.random_float()


@udf(returnType=T.BooleanType())
def random_bool():
    """
    Returns a random bool

    Examples:
        True

    Returns:
        bool
    """
    return dg.random_bool()


def random_data_df(spark_session: SparkSession, record_count: int = 20) -> DataFrame:
    """
    Random data generated for different types of data. This has no connection to real data.
    Any representation of real world data is merely a coincidence.

    Examples:
        root
            |-- id: integer (nullable = false) 0
            |-- ipv4_address: string (nullable = false) 185.236.185.101\n
            |-- ipv6_address: string (nullable = false) 63c1:1f6b:4d00:24ce:7c93:62c9:4bbc:a34b\n
            |-- lat_lon: array (nullable = false) (-53.36, 137.28)\n
            |------ element: float (containsNull = true)
            |-- record_action: string (nullable = false) insert\n
            |-- integer_val: integer (nullable = false) 7797\n
            |-- float_val: float (nullable = false) 97.3444301554033\n
            |-- bool_val: boolean (nullable = true) False\n

    Args:
        spark_session (SparkSession): The spark session used to generate the dataframe
        record_count (int): The number of records to generate. Default 20

    Returns:
        DataFrame
    """
    # @formatter:off
    random_data_schema = T.StructType() \
        .add(T.StructField(nullable=False, dataType=T.IntegerType(),            name="id")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),             name="ipv4_address")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),             name="ipv6_address")) \
        .add(T.StructField(nullable=False, dataType=T.ArrayType(T.FloatType()), name="lat_lon")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),             name="record_action")) \
        .add(T.StructField(nullable=False, dataType=T.IntegerType(),            name="integer_val")) \
        .add(T.StructField(nullable=False, dataType=T.FloatType(),              name="float_val")) \
        .add(T.StructField(nullable=True,  dataType=T.BooleanType(),            name="bool_val"))
    # @formatter:on

    collection = []
    for i in range(record_count):
        collection.append(dg.RandomData().generate(identity_id=i))

    return spark_session.createDataFrame(data=collection, schema=random_data_schema)


def address_df(spark_session, record_count: int = 20) -> DataFrame:
    """
    Random data generated for address data. This has no connection to real data.
    Any representation of real world data is merely a coincidence.

    Examples:
        root
            |-- id: integer (nullable = false) 0\n
            |-- building_number: string (nullable = false) 7811\n
            |-- city: string (nullable = false) East Christy\n
            |-- city_suffix: string (nullable = false) berg\n
            |-- country: string (nullable = false) Cote d'Ivoire\n
            |-- country_code: string (nullable = false) LB\n
            |-- current_country: string (nullable = false) United States\n
            |-- postcode: string (nullable = false) 17189\n
            |-- street_address: string (nullable = false) 152 Daniel Ports\n
            |-- street_name: string (nullable = false) Darlene Common\n
            |-- street_suffix: string (nullable = false) Station\n

    Args:
        spark_session (SparkSession): The spark session used to generate the dataframe
        record_count (int): The number of records to generate. Default 20

    Returns:
        DataFrame
    """
    # @formatter:off
    address_schema = T.StructType() \
        .add(T.StructField(nullable=False, dataType=T.IntegerType(), name="id")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="building_number")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="city")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="city_suffix")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="country")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="country_code")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="current_country")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="postcode")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="street_address")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="street_name")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="street_suffix"))
    # @formatter:on

    collection = []
    for i in range(record_count):
        collection.append(dg.Address().generate(identity_id=i))

    return spark_session.createDataFrame(data=collection, schema=address_schema)


def person_df(spark_session, record_count: int = 20) -> DataFrame:
    """
    Random data generated for a typical and non-binary person data. This has no connection to real data.
    Any representation of real world data is merely a coincidence.

    Examples:
        root
            |-- id: integer (nullable = false) 0\n
            |-- first_name: string (nullable = false) Amanda\n
            |-- first_name_non_binary: string (nullable = false) Justin\n
            |-- language_name: string (nullable = false) Cornish\n
            |-- last_name: string (nullable = false) West\n
            |-- last_name_non_binary: string (nullable = false) Martin\n
            |-- name: string (nullable = false) William Lewis\n
            |-- name_non_binary: string (nullable = false) Ricardo Barnett\n
            |-- prefix: string (nullable = false) Dr.\n
            |-- prefix_non_binary: string (nullable = false) Mx.\n
            |-- ssn: string (nullable = false) 479-20-9907\n
            |-- ssn_non_binary: string (nullable = false) 545-69-3147\n
            |-- suffix: string (nullable = false) PhD\n
            |-- suffix_non_binary: string (nullable = false) Jr.\n

    Args:
        spark_session (SparkSession): The spark session used to generate the dataframe
        record_count (int): The number of records to generate. Default 20

    Returns:
        DataFrame
    """
    # @formatter:off
    person_schema = T.StructType() \
        .add(T.StructField(nullable=False, dataType=T.IntegerType(), name="id")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="first_name")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="first_name_non_binary")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="language_name")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="last_name")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="last_name_non_binary")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="name")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="name_non_binary")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="prefix")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="prefix_non_binary")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="ssn")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="ssn_non_binary")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="suffix")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="suffix_non_binary"))
    # @formatter:on

    collection = []
    for i in range(record_count):
        collection.append(dg.Person().generate(identity_id=i))

    return spark_session.createDataFrame(data=collection, schema=person_schema)


def credit_card_df(spark_session, record_count: int = 20) -> DataFrame:
    """
    Random data generated for credit card data. This has no connection to real data.
    Any representation of real world data is merely a coincidence.

    Examples:
        root
            |-- id: integer (nullable = false) 0\n
            |-- credit_card_expire: string (nullable = false) 06/25\n
            |-- credit_card_full: string (nullable = false) VISA 16 digit\\\\nNathaniel Jackson\\\\n
            4883413840542046 06/25\\\\nCVC: 581\n
            |-- credit_card_number: string (nullable = false) 4883413840542046\n
            |-- credit_card_provider: string (nullable = false) VISA 16 digit\n
            |-- credit_card_security_code: string (nullable = false) 581\n
            |-- name_on_card: string (nullable = false) Nathaniel Jackson\n

    Args:
        spark_session (SparkSession): The spark session used to generate the dataframe
        record_count (int): The number of records to generate. Default 20

    Returns:
        DataFrame
    """
    # @formatter:off
    credit_card_schema = T.StructType() \
        .add(T.StructField(nullable=False, dataType=T.IntegerType(), name="id")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="credit_card_expire")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="credit_card_full")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="credit_card_number")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="credit_card_provider")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="credit_card_security_code")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),  name="name_on_card"))
    # @formatter:on

    collection = []
    for i in range(record_count):
        collection.append(dg.CreditCard().generate(identity_id=i))

    return spark_session.createDataFrame(data=collection, schema=credit_card_schema)


def transaction_df(spark_session, record_count: int = 20) -> DataFrame:
    """
    Random data generated for transaction data. This has no connection to real data.
    Any representation of real world data is merely a coincidence.

    Examples:
        root
            |-- id: integer (nullable = false) 0\n
            |-- username: string (nullable = false) michaeljones\n
            |-- currency: string (nullable = false) EUR\n
            |-- amount: integer (nullable = false) 113622\n

    Args:
        spark_session (SparkSession): The spark session used to generate the dataframe
        record_count (int): The number of records to generate. Default 20

    Returns:
        DataFrame
    """
    # @formatter:off
    transaction_schema = T.StructType() \
        .add(T.StructField(nullable=False, dataType=T.IntegerType(), name="id")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(), name="username")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(), name="currency")) \
        .add(T.StructField(nullable=False, dataType=T.IntegerType(), name="amount"))
    # @formatter:on

    collection = []
    for i in range(record_count):
        collection.append(dg.Transaction().generate(identity_id=i))

    return spark_session.createDataFrame(data=collection, schema=transaction_schema)


def profile_df(spark_session, record_count: int = 20) -> DataFrame:
    """
    Random data generated for personal profile data. This has no connection to real data.
    Any representation of real world data is merely a coincidence.

    Examples:
        root
            |-- id: integer (nullable = false) 0\n
            |-- job: string (nullable = false) Quality manager\n
            |-- company: string (nullable = false) Walters Inc\n
            |-- ssn: string (nullable = false) 026-69-6836\n
            |-- residence: string (nullable = false) 50529 Summers Underpass Suite 806\\\\nTownsville, IL 92545\n
            |-- current_location: array (nullable = false) [-74, -2]\n
            |------  element: decimal(10,0) (containsNull = true)
            |-- blood_group: string (nullable = false) O-\n
            |-- website: string (nullable = false) http://www.norris-taylor.com/\n
            |-- username: string (nullable = false) harriscassidy\n
            |-- name: string (nullable = false) Michael Diaz\n
            |-- sex: string (nullable = false) M\n
            |-- address: string (nullable = false) 933 Ashley Fort Suite 955\nNew Adrian, NV 32030\n
            |-- email: string (nullable = false) heathermurphy@hotmail.com\n
            |-- birthdate: date (nullable = false) 1906-10-21\n

    Args:
        spark_session (SparkSession): The spark session used to generate the dataframe
        record_count (int): The number of records to generate. Default 20

    Returns:
        DataFrame
    """
    # @formatter:off
    profile_schema = T.StructType() \
        .add(T.StructField(nullable=False, dataType=T.IntegerType(),              name="id")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),               name="job")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),               name="company")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),               name="ssn")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),               name="residence")) \
        .add(T.StructField(nullable=False, dataType=T.ArrayType(T.DecimalType()), name="current_location")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),               name="blood_group")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),               name="website")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),               name="username")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),               name="name")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),               name="sex")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),               name="address")) \
        .add(T.StructField(nullable=False, dataType=T.StringType(),               name="email")) \
        .add(T.StructField(nullable=False, dataType=T.DateType(),                 name="birthdate"))
    # @formatter:on

    collection = []
    for i in range(record_count):
        collection.append(dg.Profile().generate(identity_id=i))

    return spark_session.createDataFrame(data=collection, schema=profile_schema)


def sample_data_df(spark_session, record_count: int = 20) -> DataFrame:
    """
    Random data generated based on PySpark functions. This has no connection to real data.
    Any representation of real world data is merely a coincidence.

    Examples:
        root
            |-- id: long (nullable = false): 0\n
            |-- sin: double (nullable = true): 0.0\n
            |-- sha1: string (nullable = false): b6589fc6ab0dc82cf12099d1c2d40ab994e8410c\n
            |-- current_date: date (nullable = false): 2022-10-19\n
            |-- increasing_date: date (nullable = false): 2022-10-19\n
            |-- current_timestamp: timestamp (nullable = false): 2022-10-19 09:36:48.078\n
            |-- base64: string (nullable = false): MA==\n
            |-- ascii: integer (nullable = false): 48\n
            |-- random_int: integer (nullable = true): 2917\n
            |-- random_float: float (nullable = true): 1.93361658E11\n

    Args:
        spark_session (SparkSession): The spark session used to generate the dataframe
        record_count (int): The number of records to generate. Default 20

    Returns:
        DataFrame
    """
    return spark_session.range(0, record_count, 1) \
        .select(F.col("id"),
                F.sin(F.col("id")).alias("sin"),
                F.sha1(F.bin(F.col("id"))).alias("sha1"),
                F.current_date().alias("current_date"),
                (F.current_date() + F.col("id").cast(T.IntegerType())).alias("increasing_date"),
                F.current_timestamp().alias("current_timestamp"),
                F.base64(F.bin(F.col("id"))).alias("base64"),
                F.ascii(F.col("id")).alias("ascii"),
                random_int().alias("random_int"),
                random_float().alias("random_float"))


def all_datatype_df(spark_session):
    """
    Static data for each PySpark datatype. This has no connection to real data.
    Any representation of real world data is merely a coincidence.

    Examples:
        root
            |-- col_array: array (nullable = true) [a, 1, 1.6]\n
            |------ element: string (containsNull = true)\n
            |-- col_binary: binary (nullable = true) [DA 5A 2F E8]\n
            |-- col_boolean: boolean (nullable = true) true\n
            |-- col_byte: byte (nullable = true) 49\n
            |-- col_date: date (nullable = true) 2022-10-19\n
            |-- col_decimal: decimal(10,0) (nullable = true) 1\n
            |-- col_double: double (nullable = true) 1.1\n
            |-- col_float: float (nullable = true) 1.1\n
            |-- col_integer: integer (nullable = true) 1\n
            |-- col_long: long (nullable = true) -9223372036854775807\n
            |-- col_map: map (nullable = true) [Class -> First, Age -> 7, Name -> Zara]\n
            |------ key: string\n
            |------ value: string (valueContainsNull = true)\n
            |-- col_null: null (nullable = true) null\n
            |-- col_short: short (nullable = true) 1\n
            |-- col_string: string (nullable = true) a\n
            |-- col_struct: struct (nullable = true) [a, 1, 1.6]\n
            |------ struct_col_string: string (nullable = true)\n
            |------ struct_col_integer: integer (nullable = true)\n
            |------ struct_col_float: float (nullable = true)\n
            |-- col_timestamp: timestamp (nullable = true) 2022-10-19 15:58:14.400711\n

    Args:
        spark_session (SparkSession): The spark session used to generate the dataframe

    Returns:
        DataFrame
    """
    # @formatter:off
    complex_schema = [
        (T.StructField(nullable=True, dataType=T.StringType() , name="struct_col_string")),
        (T.StructField(nullable=True, dataType=T.IntegerType(), name="struct_col_integer")),
        (T.StructField(nullable=True, dataType=T.FloatType()  , name="struct_col_float"))]

    data_type_schema = T.StructType() \
        .add(T.StructField(nullable=True, dataType=T.ArrayType(T.StringType(), True)        , name="col_array")) \
        .add(T.StructField(nullable=True, dataType=T.BinaryType()                           , name="col_binary")) \
        .add(T.StructField(nullable=True, dataType=T.BooleanType()                          , name="col_boolean")) \
        .add(T.StructField(nullable=True, dataType=T.ByteType()                             , name="col_byte")) \
        .add(T.StructField(nullable=True, dataType=T.DateType()                             , name="col_date")) \
        .add(T.StructField(nullable=True, dataType=T.DecimalType()                          , name="col_decimal")) \
        .add(T.StructField(nullable=True, dataType=T.DoubleType()                           , name="col_double")) \
        .add(T.StructField(nullable=True, dataType=T.FloatType()                            , name="col_float")) \
        .add(T.StructField(nullable=True, dataType=T.IntegerType()                          , name="col_integer")) \
        .add(T.StructField(nullable=True, dataType=T.LongType()                             , name="col_long")) \
        .add(T.StructField(nullable=True, dataType=T.MapType(T.StringType(), T.StringType()), name="col_map")) \
        .add(T.StructField(nullable=True, dataType=T.NullType()                             , name="col_null")) \
        .add(T.StructField(nullable=True, dataType=T.ShortType()                            , name="col_short")) \
        .add(T.StructField(nullable=True, dataType=T.StringType()                           , name="col_string")) \
        .add(T.StructField(nullable=True, dataType=T.StructType(complex_schema)             , name="col_struct")) \
        .add(T.StructField(nullable=True, dataType=T.TimestampType()                        , name="col_timestamp"))

    data_type_data = [
        (["a", "1", "1.6"], os.urandom(4),  True,  b"1"[0], datetime.datetime.today(), decimal.Decimal(1.1), 1.1, 1.1, 1, -9223372036854775807, {"Name": "Zara", "Age": 7, "Class": "First"}, None, 1, "a", ["a", 1, 1.6], datetime.datetime.utcnow()),
        (["b", "2", "1.7"], os.urandom(4),  False, b"2"[0], datetime.datetime.today(), decimal.Decimal(2.2), 2.2, 2.2, 2,     -372036854775807, {"Name": "Zara", "Age": 7, "Class": "First"}, None, 2, "b", ["b", 2, 1.7], datetime.datetime.utcnow()),
        (["c", "3", "1.8"], os.urandom(4),  True,  b"3"[0], datetime.datetime.today(), decimal.Decimal(3.3), 3.3, 3.3, 3,       -2036854775807, {"Name": "Zara", "Age": 7, "Class": "First"}, None, 3, "c", ["c", 3, 1.8], datetime.datetime.utcnow()),
        (["d", "4", "1.9"], os.urandom(4),  False, b"4"[0], datetime.datetime.today(), decimal.Decimal(4.4), 4.4, 4.4, 4,            -54775807, {"Name": "Zara", "Age": 7, "Class": "First"}, None, 4, "d", ["d", 4, 1.9], datetime.datetime.utcnow()),
        (["e", "5", "1.0"], os.urandom(4),  True,  b"5"[0], datetime.datetime.today(), decimal.Decimal(5.5), 5.5, 5.5, 5,                    0, {"Name": "Zara", "Age": 7, "Class": "First"}, None, 5, "e", ["e", 5, 1.0], datetime.datetime.utcnow()),
        (["a", "1", "1.6"], os.urandom(4),  True,  b"6"[0], datetime.datetime.today(), decimal.Decimal(6.6), 6.6, 6.6, 6,                    0, {"Name": "Zara", "Age": 7, "Class": "First"}, None, 6, "f", ["a", 1, 1.6], datetime.datetime.utcnow()),
        (["b", "2", "1.7"], os.urandom(4),  False, b"7"[0], datetime.datetime.today(), decimal.Decimal(7.7), 7.7, 7.7, 7,             54775807, {"Name": "Zara", "Age": 7, "Class": "First"}, None, 7, "g", ["b", 2, 1.7], datetime.datetime.utcnow()),
        (["c", "3", "1.8"], os.urandom(4),  True,  b"8"[0], datetime.datetime.today(), decimal.Decimal(8.8), 8.8, 8.8, 8,        2036854775807, {"Name": "Zara", "Age": 7, "Class": "First"}, None, 8, "h", ["c", 3, 1.8], datetime.datetime.utcnow()),
        (["d", "4", "1.9"], os.urandom(4),  False, b"9"[0], datetime.datetime.today(), decimal.Decimal(9.9), 9.9, 9.9, 9,      372036854775807, {"Name": "Zara", "Age": 7, "Class": "First"}, None, 9, "i", ["d", 4, 1.9], datetime.datetime.utcnow()),
        (["e", "5", "1.0"], os.urandom(4),  True,  b"0"[0], datetime.datetime.today(), decimal.Decimal(0.0), 0.0, 0.0, 0,  9223372036854775807, {"Name": "Zara", "Age": 7, "Class": "First"}, None, 0, "j", ["e", 5, 1.0], datetime.datetime.utcnow())]
    # @formatter:on
    return spark_session.createDataFrame(data=data_type_data, schema=data_type_schema)


if __name__ == '__main__':
    job_start_time = datetime.datetime.now()
    spark = get_spark(application_name="helper_spark_data_generator.py")
    print_header(f"               Start Time ► {job_start_time}\n"
                 f"Time To Get Spark Context ► {datetime.datetime.now() - job_start_time}\n"
                 f"     Spark Application ID ► {spark.sparkContext.applicationId}\n"
                 f"           Spark App Name ► {spark.sparkContext.appName}\n"
                 f"            Spark Version ► {spark.version}\n"
                 f"               Spark User ► {spark.sparkContext.sparkUser()}")
    dataframe_list = {"random_data_df": random_data_df,
                      "address_df": address_df,
                      "person_df": person_df,
                      "credit_card_df": credit_card_df,
                      "transaction_df": transaction_df,
                      "profile_df": profile_df,
                      "sample_data_df": sample_data_df,
                      "all_datatype_df": all_datatype_df}
    for df_name, df in dataframe_list.items():
        print_header(df_name)
        x = df(spark_session=spark)
        x.show(truncate=False, vertical=False)
        x.printSchema()
    print_header(f"               Start Time ► {job_start_time}\n"
                 f"             Job Duration ► {datetime.datetime.now() - job_start_time}\n"
                 f"                 End Time ► {datetime.datetime.now()}\n")
