import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def main(inputs, output):
    reviews_schema = types.StructType([
        types.StructField('review_id', types.StringType()),
        types.StructField('user_id', types.StringType()),
        types.StructField('business_id', types.StringType()),
        types.StructField('stars', types.FloatType()),
        types.StructField('useful', types.FloatType()),
        types.StructField('funny', types.FloatType()),
        types.StructField('cool', types.FloatType()),
        types.StructField('date', types.DateType()),
    ])

    comments = spark.read.json(inputs, schema=reviews_schema)
    new_df = comments.select('*')
    n_df = new_df.withColumn('id', new_df['user_id'])
    df = n_df.select('id', 'business_id', 'user_id', 'stars')
    df.write.json(output, mode='overwrite')



if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('yelp').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)