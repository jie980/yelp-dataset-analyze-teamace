import sys
import schema
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions, types
def main(input1,input2,outputs):
    df_business = spark.read.json(input1,schema=schema.business_schema)
    df_business = df_business.select('business_id','state','city').cache()
    df_review = spark.read.json(input2,schema=schema.reviews_schema).repartition(32)
    df_review = df_review.select('review_id',df_review['business_id'].alias('business_id2'))
    df = df_business.join(df_review,df_business['business_id']==df_review['business_id2']).cache()
    df_state = df.groupby('state').agg(functions.count('review_id').alias("review_number"))
    df_state.write.json(outputs+'/reviews_states',mode='overwrite')
if __name__ == '__main__':
    input1 = sys.argv[1]
    input2 = sys.argv[2]
    outputs = sys.argv[3]
    spark = SparkSession.builder.appName('preprocess_reviews').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input1,input2, outputs)