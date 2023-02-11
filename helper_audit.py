"""
    This module is used to audit spark data jobs. It contains required audits for every job and allows for custom
    audits particular to that job.
"""
# Standard Library Imports <- This is not required but useful to separate out imports
from helper_spark import get_spark
# keeping these as uppercase despite the ide warning. This is a typical convention in pyspark code. It is common
# enough to perpetuate
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
import json


class Audit:
    def __init__(self):
        self.input = AuditValue()
        self.output = AuditValue()
        self.result = AuditResult()

    def add_audit(self, key, default_value):
        """
        Add a custom audit to the input and output attribute with a default value

        args:
            key (str): Name of the custom audit. Typically, the column name and action taken (sum, count, etc)
            default_value: The default value of the audit

        Examples:
            audit.add_audit(key="custom_audit", default_value=-1)

        Returns:
            Void
        """
        self.input.add_audit(key=key, default_value=default_value)
        self.output.add_audit(key=key, default_value=default_value)

    def get_dataframe(self, spark_session: SparkSession = False):
        """
        Returns the audit values as a dataframe

        args:
            spark_session (SparkSession): Allows for a spark session to be used

        Returns:
            DataFrame(key, value, source)
        """
        dict_schema = T.StructType() \
            .add(field="key", data_type=T.StringType(), nullable=False) \
            .add(field="value", data_type=T.StringType(), nullable=True)

        if not spark_session:
            _spark = get_spark(application_name="Create Audit Dataframe")
        else:
            _spark = spark_session
        _input_df = _spark \
            .createDataFrame(data=self.input.audits.items(), schema=dict_schema, verifySchema=True) \
            .withColumn("source", lit("input"))
        _output_df = _spark \
            .createDataFrame(data=self.output.audits.items(), schema=dict_schema, verifySchema=True) \
            .withColumn("source", lit("output"))
        _result_df = _spark \
            .createDataFrame(data=self.result.audits.items(), schema=dict_schema, verifySchema=True) \
            .withColumn("source", lit("result"))
        return _input_df.unionAll(_output_df).unionAll(_result_df)

    def print(self) -> None:
        """
        Pretty prints the contents of the audit for input, output, and result

        Returns:
            Void
        """
        _input = json.dumps(self.input.audits, indent=2)
        print("Input")
        self.input.print()

        print("Output")
        self.output.print()

        print("Result")
        self.result.print()

    def show(self) -> None:
        """
        Alias for print()

        Returns:
            Void
        """
        self.print()

    def initialize_from_df(self, df: DataFrame, populate_input: bool = False):
        """
        Adds custom audits optionally populates the input attribute with the values from a spark dataframe

        Args:
            df (DataFrame): The dataframe to extract the key and value from
            populate_input: Flag indicating that the values found in the dataframe should be used to populate the value

        Returns:
            Void
        """
        _row = df.first()
        for c in df.columns:
            if populate_input:
                self.input.add_audit(key=c, default_value=_row[c])
            else:
                self.input.add_audit(key=c, default_value=None)
            self.output.add_audit(key=c, default_value=None)


class AuditValue:
    def __init__(self):
        self.audits = {"record_count": None,
                       "insert_count": None,
                       "update_count": None,
                       "delete_count": None,
                       "no_change_count": None}

    def add_audit(self, key, default_value):
        """
        Add a custom audit with a default value

        args:
            key (str): Name of the custom audit. Typically, the column name and action taken (sum, count, etc)
            default_value: The default value of the audit

        Examples:
            audit.input.add_audit(key="custom_audit", default_value=-1)
            audit.output.add_audit(key="custom_audit", default_value=-1)

        Returns:
            Void
        """
        self.audits.update({key: default_value})

    def set_record_count_value(self, value):
        """
        Sets the audit value for the builtin `record_count` key

        Args:
            value: The number of records

        Returns:
            Void
        """
        self.audits["record_count"] = value

    def set_insert_count_audit_value(self, value):
        """
        Sets the audit value for the builtin `insert_count` key

        Args:
            value: The number of records

        Returns:
            Void
        """
        self.audits["insert_count"] = value

    def set_update_count_audit_value(self, value):
        """
        Sets the audit value for the builtin `update_count` key

        Args:
            value: The number of records

        Returns:
            Void
        """
        self.audits["update_count"] = value

    def set_delete_count_audit_value(self, value):
        """
        Sets the audit value for the builtin `delete_count` key

        Args:
            value: The number of records

        Returns:
            Void
        """
        self.audits["delete_count"] = value

    def set_no_change_count_audit_value(self, value):
        """
        Sets the audit value for the builtin `no_change_count` key

        Args:
            value: The number of records

        Returns:
            Void
        """
        self.audits["no_change_count"] = value

    def set_audit_value(self, key, value):
        """
        Sets the audit value for the custom key

        Args:
            key: The key to set the value for
            value: The value for the specified key

        Returns:
            Void
        """
        self.audits[key] = value

    def get_record_count_value(self):
        """
        Gets the audit value for the builtin `record_count` key

        Returns:
            Void
        """
        return self.audits["record_count"]

    def get_insert_count_audit_value(self):
        """
        Gets the audit value for the builtin `insert_count` key

        Returns:
            Void
        """
        return self.audits["insert_count"]

    def get_update_count_audit_value(self):
        """
        Gets the audit value for the builtin `update_count` key

        Returns:
            Void
        """
        return self.audits["update_count"]

    def get_delete_count_audit_value(self):
        """
        Gets the audit value for the builtin `delete_count` key

        Returns:
            Void
        """
        return self.audits["delete_count"]

    def get_no_change_count_audit_value(self):
        """
        Gets the audit value for the builtin `no_change_count` key

        Returns:
            Void
        """
        return self.audits["no_change_count"]

    def get_audit_value(self, key):
        """
        Gets the audit value for the custom key

        Args:
            key: The key to get the value for

        Returns:
            Any
        """
        return self.audits[key]

    def populate_from_df(self, df: DataFrame):
        """
        Sets the audit values for each of the columns in the DataFrame. Only takes the first row. If column (key) is
        not found, it adds the audit and value.

        Args:
            df (DataFrame): The dataframe to extract the key (column) and value from

        Returns:
            Void
        """
        _row = df.first()
        for c in df.columns:
            if c in self.audits.keys():
                self.set_audit_value(key=c, value=_row[c])
            else:
                self.add_audit(key=c, default_value=_row[c])

    def print(self) -> None:
        """
        Pretty prints the contents of the audit

        Returns:
            Void
        """
        _input = json.dumps(self.audits, indent=2)
        print(_input)

    def show(self) -> None:
        """
        Alias for print()

        Returns:
            Void
        """
        self.print()


class AuditResult:
    def __init__(self):
        self.audits = {"audit_passed": bool(False),
                       "audit_outcome": str("Audit Failed Num Was Not Set"),
                       "num_audits_passed": 0,
                       "num_audits_failed": 0,
                       "audits_performed": dict()}

    def set_audits_performed(self, audit_dict: dict):
        """
        Sets the value `audits_performed`. A dictionary of audit action and result. Sets the value for
        `num_audits_passed`, `num_audits_failed`, `audit_passed`, and `audit_outcome`

        Examples:
            audit_dict =
                {
                    "Check Input Record Counts": false,
                    "Check Output Record Counts Match Input Record Count": false,
                    "Check Output Amount Percent Total Sum = 100%": false,
                    "Check Output Has No Change Records": true,
                    "Check Output Updated Records Are All Uppercase": true
                }

        Args:
            audit_dict (dict): Dictionary to add to the attribute

        Returns:
            Void
        """
        self.audits["audits_performed"] = audit_dict
        _passed_count = 0
        _failed_count = 0
        for a in audit_dict.values():
            if a:
                _passed_count += 1
            else:
                _failed_count += 1
        self.set_audit_result(passed_count=_passed_count, failed_count=_failed_count)

    def set_audit_result(self, passed_count: int, failed_count: int):
        """
        Sets the value for `num_audits_passed` and `num_audits_failed`. sets the audit value for
        `audit_passed` and `audit_outcome`

        Args:
            passed_count (int): The number of audits that passed
            failed_count (int): The number of audits that failed

        Returns:
            Void
        """
        self.set_num_audits_passed(passed_count=passed_count)
        self.set_num_audits_failed(failed_count=failed_count)

    def set_num_audits_passed(self, passed_count: int):
        """
        Sets the value for the builtin `num_audits_passed`

        Args:
            passed_count (int): The number of audits that passed

        Returns:
            Void
        """
        self.audits["num_audits_passed"] = passed_count

    def set_num_audits_failed(self, failed_count: int):
        """
        Sets the value for `num_audits_failed`. sets the audit attribute for
        `audit_passed` and `audit_outcome`

        Args:
            failed_count (int): The number of audits that failed

        Returns:
            Void
        """
        self.audits["num_audits_failed"] = failed_count
        if failed_count == 0:
            self.audits["audit_passed"] = True
            self.audits["audit_outcome"] = "Audit(s) Passed"
        else:
            self.audits["audit_passed"] = False
            self.audits["audit_outcome"] = "Audit(s) Failed"

    def get_audit_passed(self):
        """
        Gets the value for `audit_passed`

        Returns:
            Void
        """
        return self.audits["audit_passed"]

    def get_audit_outcome(self):
        """
        Gets the value for `audit_outcome`

        Returns:
            Void
        """
        return self.audits["audit_outcome"]

    def get_audits_num_passed(self):
        """
        Gets the value for `num_audits_passed`

        Returns:
            Void
        """
        return self.audits["num_audits_passed"]

    def get_audits_num_failed(self):
        """
        Gets the value for `num_audits_failed`

        Returns:
            Void
        """
        return self.audits["num_audits_failed"]

    def print(self) -> None:
        """
        Pretty prints the contents of the audit

        Returns:
            Void
        """
        print(json.dumps(self.audits, indent=2))

    def show(self) -> None:
        """
        Alias for print()

        Returns:
            Void
        """
        self.print()


audit = Audit()

if __name__ == '__main__':
    audit.add_audit(key="custom_audit", default_value=-1)
    audit.input.set_no_change_count_audit_value(999)
    audit.output.set_no_change_count_audit_value(888)

    audit.input.add_audit("only_in_input", "input")
    audit.output.add_audit("only_in_output", None)
    audit.print()

    import helper_spark_data_generator as sdg

    spark = get_spark(application_name="Create Audit Sample Dataframe")
    input_df = sdg.transaction_df(spark_session=spark, record_count=11)
    input_audit_df = input_df \
        .agg(F.count("id").alias("record_count"),
             F.sum("amount").alias("amount_sum"),
             F.countDistinct("id").alias("id_distinct"))

    audit.initialize_from_df(df=input_audit_df, populate_input=True)
    audit.show()
    audit.output.populate_from_df(input_audit_df)
    audit.output.set_audit_value(key="amount_sum", value="-999")
    audit.output.show()
    audit.result.set_num_audits_passed(passed_count=2)
    audit.result.set_num_audits_failed(failed_count=0)
    audit.result.show()
    audit.result.set_audit_result(passed_count=12, failed_count=1)
    audit.result.show()
    audits = {"1 Equals 2": 1 == 2,
              "1.001 Equals 1": 1.001 == 1,
              "1 Equals 1": 1 == 1,
              "2 Equals 2": 2 == 2,
              "a Equals  a": "a" == "a"}
    audit.result.set_audits_performed(audit_dict=audits)
    audit.result.show()
    audit.get_dataframe().show(n=100)
    audit.print()
