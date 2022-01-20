from pyspark.sql import SparkSession


def get_spark_session(env, name):
    if env == "DEV":
        spark = SparkSession. \
            builder. \
            appName(name). \
            master('local'). \
            getOrCreate()

    elif env=="PROD":
        spark = SparkSession. \
            builder. \
            appName(name). \
            master('yarn'). \
            getOrCreate()

    return spark


