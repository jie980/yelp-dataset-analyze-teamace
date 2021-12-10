# This script does the general cleaning process in the user dataset, including:
# 1. Delete the rows which includes the none value
# 2. Based the length of the elite, add a column named num_elite


# ATTENTION: You have to add a column num_elite when create redshift copying from s3

# Run spark-submit user_cleaning_script.py <json_file_name>
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re
from pyspark.sql import functions, types
# add more functions as necessary


user_schema = types.StructType([
    types.StructField('user_id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('review_count', types.StringType()),
    types.StructField('yelping_since', types.DateType()),
    types.StructField('useful', types.IntegerType()),
    types.StructField('funny', types.IntegerType()),
    types.StructField('cool', types.IntegerType()),
    types.StructField('elite', types.StringType()),
    types.StructField('fans', types.IntegerType()),
    types.StructField('average_stars', types.FloatType()),
])

def main(inputs):
    
    res = spark.read.json(inputs,schema=user_schema)
    # drop na lines
    res = res.na.drop('any')
    # add a column
    res = res.withColumn('num_elite',functions.when(functions.length(res.elite)==0, 0).otherwise(functions.size(functions.split(res.elite, ','))))
    res.write.json('user_modified',mode ='append')
    
if __name__ == '__main__':
    inputs = sys.argv[1]

    spark = SparkSession.builder.appName('user_general_cleaning').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)