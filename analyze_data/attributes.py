import sys
import schema
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions, types
def main(input1,input2,outputs):
    df_business = spark.read.json(input1, schema=schema.business_schema).cache()
    df_review = spark.read.json(input2, schema=schema.reviews_schema).repartition(32)
    df_review = df_review.select('review_id',df_review['business_id'].alias('business_id2'),'stars').cache()

    df_business_dogs =df_business.select('business_id','attributes.DogsAllowed')
    df_business_dogs = df_business_dogs.filter(df_business_dogs['DogsAllowed'].isNotNull()).filter(df_business_dogs['DogsAllowed'] != 'None')
    df_dogsallowed  = df_business_dogs.join(df_review,df_business_dogs['business_id']==df_review['business_id2']).cache()
    df_dogsallowed = df_dogsallowed.groupby('DogsAllowed').agg(functions.avg('stars').alias('stars'))
    df_dogsallowed = df_dogsallowed.withColumnRenamed('DogsAllowed','dogsallowed')
    df_dogsallowed.write.json(outputs+'/dogsallowed_stars',mode='overwrite')

    df_business_wifi = df_business.select('business_id','attributes.WiFi')
    df_business_wifi = df_business_wifi.filter(df_business_wifi['WiFi'].isNotNull()).filter(df_business_wifi['WiFi'] != 'None')
    df_business_wifi = df_business_wifi.withColumn('wifi', functions.regexp_replace('WiFi', "u'paid'", "'paid'"))\
        .withColumn('wifi', functions.regexp_replace('WiFi', "u'no'", "'no'"))\
        .withColumn('wifi', functions.regexp_replace('WiFi', "u'free'", "'free'"))

    df_wifi = df_business_wifi.join(df_review,df_business_wifi['business_id']==df_review['business_id2'])
    df_wifi = df_wifi.groupby('wifi').agg(functions.avg('stars').alias('stars'))
    df_wifi.write.json(outputs+'/wifi_stars',mode='overwrite')

    df_business_noisy = df_business.select('business_id','attributes.NoiseLevel')
    df_business_noisy = df_business_noisy.filter(df_business_noisy['NoiseLevel'].isNotNull()).filter(df_business_noisy['NoiseLevel'] != 'None')
    df_business_noisy = df_business_noisy.withColumn('noiselevel', functions.regexp_replace('NoiseLevel', "u'loud'", "'loud'"))\
        .withColumn('noiselevel', functions.regexp_replace('NoiseLevel', "u'very_loud'", "'very_loud'"))\
        .withColumn('noiselevel', functions.regexp_replace('NoiseLevel', "u'average'", "'average'"))\
        .withColumn('noiselevel', functions.regexp_replace('NoiseLevel', "u'quiet'", "'quiet'"))
    df_noisy = df_business_noisy.join(df_review,df_business_noisy['business_id']==df_review['business_id2'])
    df_noisy = df_noisy.groupby('noiselevel').agg(functions.avg('stars').alias('stars'))
    df_noisy.write.json(outputs+'/noiselevel_stars',mode='overwrite')
if __name__ == '__main__':
    input1 = sys.argv[1]
    input2 = sys.argv[2]
    outputs = sys.argv[3]
    spark = SparkSession.builder.appName('preprocess_reviews').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input1,input2, outputs)