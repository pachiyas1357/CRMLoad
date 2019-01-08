from pyspark.sql import Row,SparkSession
from pyspark.sql.functions import current_timestamp,monotonically_increasing_id
import os
import json
# entry point for PySpark ETL application


def extract_data(spark):
    #:param spark: Spark session object.
    #:return: Spark DataFrame.

    print (" Extract Data Started")

  #  json_conf=json.load(open("/home/hdfs/CRMDataLoad/Config/etl_config.json").read())
  #  url=json_conf["hdfsurl"]
  #  print(url)

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://192.168.56.103:3306/customer") \
        .option("user", "sqluser") \
        .option("password", "Time2018$") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", "customer") \
        .load()

    return df

def validate_data(data_stg):

    data_stg.select("zip","first_name").show()

  #  for first_name in data_stg.columns:
  #      error_df=data_stg.withColumn("ERROR",lower(col("firts_name"))

  #  print(data_stg.explain())

    return data_stg



def transform_data(data_stg):
    data_stg.dropna()
    #data_stg.fillna
    #check for zip code
    #check for email id
    #

    data_stg_trans = data_stg.withColumn("New Date", current_timestamp()).\
        withColumn("Customer UUID",monotonically_increasing_id())

    return data_stg_trans

if __name__ == "__main__":
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars hdfs://nmaster:8020/user/hive/warehouse/mysql-connector-java-5.1.47.jar  --master local[4]  pyspark-shell'

    print("starting Application")

    spark = SparkSession \
        .builder \
        .appName("Second App") \
        .enableHiveSupport() \
        .config("spark.sql.warehouse.dir", "/warehouse/tablespace/managed/hive") \
        .getOrCreate()


    # execute ETL pipeline
    data_stg = extract_data(spark)
      # log the success and terminate Spark application
    data_valid= validate_data(data_stg)

    data_trans=transform_data(data_valid)

    data_trans.printSchema()
    data_trans.show()


    print('test_etl_job is finished')
    spark.stop()
