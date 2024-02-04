from pyspark.sql import SparkSession

def get_spark_version():
    spark = SparkSession.builder.appName("SparkVersion").getOrCreate()
    return spark.version

if __name__ == "__main__":
    version = get_spark_version()
    print("Spark version:", version)