# The script extracts the categories and via inner join to construct categories-review-number table
# The script has three 3 inputs, business data, review data, word count of category data
# The script has a side output named test1

# corresponding Redshift sql
# ```
# CREATE TABLE reviews (
    # categories_name CHAR(100),
    # sum_reviews FLOAT,
    # good_reviews FLOAT,
    # good_rate FLOAT,
    # num FLOAT
# );

# COPY reviews FROM 's3://c732-hpa61-a5-haomingpan/ProjectYelp/yelp_academic_dataset_review.json'
# IAM_ROLE 'arn:aws:iam::996621308891:role/RedshiftS3Full'
# FORMAT JSON 'auto';
# ```
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import functions, types
from pyspark import SparkConf, SparkContext
import json
import schema

def words_once(line):
    text = json.loads(line)
    for i in text['categories'].split(', '):
        if text['stars'] > 4:
            yield (i, [1, 1])
        else: 
            yield (i, [1, 0])

def add_pairs(x,y):
    return (x[0] + y[0], x[1] + y[1])

def get_key(kv):
    return kv[0]
def get_value(kv):
    return kv[1][0]

def main(input1,input2,input3, output):
    # main logic starts here
    # './newdata/business'
    df_business = spark.read.json(input1,schema=schema.business_schema)
    df_business = df_business.select('business_id','name','state','categories').cache()
    # ./newdata/yelp_academic_dataset_review.json
    df_review = spark.read.json(input2,schema=schema.reviews_schema).repartition(32)
    df_review = df_review.select('review_id',df_review['business_id'].alias('busid'),'stars')
    df = df_review.join(df_business,df_review['busid']==df_business['business_id'],'inner')
    df = df.select('business_id','review_id','stars','name','state','categories').sort('business_id').cache()
    df.write.json('test1',mode='overwrite')
    text = sc.textFile('./test1')
    words = text.flatMap(words_once)
    wordcount = words.reduceByKey(add_pairs)

    outdata = wordcount.sortBy(get_key,ascending=False)
    outdata = outdata.map(lambda x: (x[0], x[1][0], x[1][1], float(x[1][1])/float(x[1][0])))
    reviews_df = outdata.toDF().withColumnRenamed("_1", "category").withColumnRenamed('_2','sum_reviews').withColumnRenamed('_3','good_reviews').withColumnRenamed('_4','good_rate')
    # test
    num_df = spark.read.json(input3)
    df_overall = reviews_df.join(num_df,reviews_df['category']==num_df['categories'],'inner').drop('categories')
    df_overall.write.json(output,mode='overwrite')
if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)

    spark = SparkSession(sc)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    # business data
    input1 = sys.argv[1]
    # review data
    input2 = sys.argv[2]
    # word count of category data
    input3 = sys.argv[3]
    # output
    output = sys.argv[4]
    main(input1,input2,input3, output)