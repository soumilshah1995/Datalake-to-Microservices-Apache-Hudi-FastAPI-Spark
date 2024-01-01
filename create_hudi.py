try:
    import os
    import sys
    import uuid
    import pyspark
    from pyspark.sql import SparkSession
    from pyspark import SparkConf, SparkContext
    from faker import Faker
    import uuid
    import random

    print("Imports loaded ")

except Exception as e:
    print("error", e)

HUDI_VERSION = '0.14.0'
SPARK_VERSION = '3.4'
SUBMIT_ARGS = f"--packages org.apache.hudi:hudi-spark{SPARK_VERSION}-bundle_2.12:{HUDI_VERSION} pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .config('className', 'org.apache.hudi') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .getOrCreate()

global faker

faker = Faker()


class DataGenerator(object):

    @staticmethod
    def get_data(samples):
        return [
            (
                uuid.uuid4().__str__(),
                faker.name(),
                faker.random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
                faker.state(),
                str(faker.random_int(min=10000, max=150000)),
                str(faker.random_int(min=18, max=60)),
                str(faker.random_int(min=0, max=100000)),
                str(faker.unix_time()),
                faker.email(),
                faker.credit_card_number(card_type='amex'),
                random.choice(["2021", "2022", "2023"]),
                faker.month()

            ) for x in range(samples)
        ]


def upsert_hudi_table(
        db_name,
        table_name,
        record_id,
        precomb_key,
        spark_df,
        table_type='COPY_ON_WRITE',
        method='upsert',
):
    path = f"file:///Users/soumilshah/IdeaProjects/mdbt/tmp/{table_name}"
    print("path", path, end="\n")
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.table.type': table_type,
        'hoodie.datasource.write.recordkey.field': record_id,
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': method,
        'hoodie.datasource.write.precombine.field': precomb_key,
        'hoodie.index.type': 'RECORD_INDEX',
        'hoodie.metadata.record.index.enable': "true"
    }
    spark_df.write.format("hudi"). \
        options(**hudi_options). \
        mode("append"). \
        save(path)


data = DataGenerator.get_data(samples=100)
columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts", "email", "credit_card",
           "year", "month"]
spark_df = spark.createDataFrame(data=data, schema=columns)
spark_df.show()


upsert_hudi_table(
    db_name='hudidb',
    table_name='employees',
    record_id='emp_id',
    precomb_key='ts',
    spark_df=spark_df,
    table_type='COPY_ON_WRITE',
    method='upsert',
)
