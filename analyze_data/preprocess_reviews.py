import sys
import schema
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import datetime
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import functions, types


def main(input1,input2,outputs):
    df_business = spark.read.json(input1,schema=schema.business_schema)
    df_business = df_business.select('business_id','name','state').cache()
    df_review = spark.read.json(input2,schema=schema.reviews_schema).repartition(32)
    df_review = df_review.select('review_id',df_review['business_id'].alias('busid'),'stars')
    df = df_review.join(df_business,df_review['busid']==df_business['business_id'],'inner')
    df = df.select('business_id','review_id','stars','name','state').sort('business_id').cache()
    df_total = df.groupby(df['business_id'].alias('business_id3')).agg(functions.count('review_id').alias('total_review')).cache()
    # df_total = df_total.filter(df_total['total_review']>20)
    df_positive = df.filter(df['stars']>=4)
    df_positive = df_positive.groupby(df['business_id'].alias('business_id2')).agg(functions.count('stars').alias('good_review'))
    df_positive = df_positive.join(df_total,df_positive['business_id2']==df_total['business_id3'])
    df_positive = df_positive.join(df_business,df_positive['business_id2']==df_business['business_id'])
    df_positive = df_positive.drop('business_id2').drop('business_id3')
    df_positive = df_positive.sort(df_positive['good_review'],ascending=False)
    df_positive.show(10)
    df_positive.write.json(outputs,mode='overwrite')

if __name__ == '__main__':
    input1 = sys.argv[1]
    input2 = sys.argv[2]
    outputs = sys.argv[3]
    spark = SparkSession.builder.appName('preprocess_reviews').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input1,input2, outputs)