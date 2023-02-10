"""
    This module is used to audit spark data jobs. It contains required audits for every job and custom audits
    particular to that job.
"""
# Standard Library Imports <- This is not required but useful to separate out imports
from helper_printer import print_header
from helper_spark import get_spark
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
        self.input.add_audit(key=key, default_value=default_value)
        self.output.add_audit(key=key, default_value=default_value)

    def get_dataframe(self, spark_session: SparkSession = False):
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
        _input = json.dumps(self.input.audits, indent=2)
        print("Input")
        self.input.print()

        print("Output")
        self.output.print()

        print("Result")
        self.result.print()

    def show(self) -> None:
        self.print()

    # def run(self) -> tuple[bool, str]:
    #     audits_pass: tuple[bool, str] = True, "Pass"
    #     for input_k, input_v in self.input.audits.items():
    #         try:
    #             output_v = self.output.audits[input_k]
    #         except:
    #             output_v = None
    #         result = input_v == output_v
    #         if not result:
    #             return False, "Fail"
    #     return audits_pass

    def initialize_from_df(self, df: DataFrame, populate_input: bool = False):
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
        self.audits.update({key: default_value})

    def set_record_count_value(self, value):
        self.audits["record_count"] = value

    def set_insert_count_audit_value(self, value):
        self.audits["insert_count"] = value

    def set_update_count_audit_value(self, value):
        self.audits["update_count"] = value

    def set_delete_count_audit_value(self, value):
        self.audits["delete_count"] = value

    def set_no_change_count_audit_value(self, value):
        self.audits["no_change_count"] = value

    def set_audit_value(self, key, value):
        self.audits[key] = value

    def get_record_count_value(self):
        return self.audits["record_count"]

    def get_insert_count_audit_value(self):
        return self.audits["insert_count"]

    def get_update_count_audit_value(self):
        return self.audits["update_count"]

    def get_delete_count_audit_value(self):
        return self.audits["delete_count"]

    def get_no_change_count_audit_value(self):
        return self.audits["no_change_count"]

    def get_audit_value(self, key):
        return self.audits[key]

    def populate_from_df(self, df: DataFrame):
        _row = df.first()
        for c in df.columns:
            if c in self.audits.keys():
                self.set_audit_value(key=c, value=_row[c])
            else:
                self.add_audit(key=c, default_value=_row[c])

    def print(self) -> None:
        _input = json.dumps(self.audits, indent=2)
        print(_input)

    def show(self) -> None:
        self.print()


class AuditResult:
    def __init__(self):
        self.audits = {"audit_passed": bool(False),
                       "audit_outcome": str("Audit Failed Num Was Not Set"),
                       "num_audits_passed": 0,
                       "num_audits_failed": 0,
                       "audits_performed": dict()}

    def set_audits_performed(self, audits: dict):
        self.audits["audits_performed"] = audits
        _passed_count = 0
        _failed_count = 0
        for a in audits.values():
            if a:
                _passed_count += 1
            else:
                _failed_count += 1
        self.set_audit_result(passed=_passed_count, failed=_failed_count)

    def set_audit_result(self, passed: int, failed: int):

        self.set_num_audits_passed(value=passed)
        self.set_num_audits_failed(value=failed)

    def set_num_audits_passed(self, value: int):
        self.audits["num_audits_passed"] = value

    def set_num_audits_failed(self, value: int):
        self.audits["num_audits_failed"] = value
        if value == 0:
            self.audits["audit_passed"] = True
            self.audits["audit_outcome"] = "Audit(s) Passed"
        else:
            self.audits["audit_passed"] = False
            self.audits["audit_outcome"] = "Audit(s) Failed"

    def get_audit_passed(self):
        return self.audits["audit_passed"]

    def get_audit_outcome(self):
        return self.audits["audit_outcome"]

    def get_audits_num_passed(self):
        return self.audits["num_audits_passed"]

    def get_audits_num_failed(self):
        return self.audits["num_audits_failed"]

    def print(self) -> None:
        print(json.dumps(self.audits, indent=2))

    def show(self) -> None:
        print()


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
    audit.result.set_num_audits_passed(value=2)
    audit.result.set_num_audits_failed(value=0)
    audit.result.show()
    audit.result.set_audit_result(passed=12, failed=1)
    audit.result.show()
    audits = {"1 Equals 2": 1 == 2,
              "1.001 Equals 1": 1.001 == 1,
              "1 Equals 1": 1 == 1,
              "2 Equals 2": 2 == 2,
              "a Equals  a": "a" == "a"}
    audit.result.set_audits_performed(audits=audits)
    audit.result.show()
    audit.get_dataframe().show(n=100)
    audit.print()
