# The scripts has summaried the location distribution for 5 star restaurant
# corresponding Redshift sql
# ```
# CREATE TABLE reviews (
    # city CHAR(100),
    # num_city FLOAT,
    # state CHAR(5),
    # num_state FLOAT,
    # longitude FLOAT,
    # latitude FLOAT
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


def main(input1, output):
    # main logic starts here
    # './newdata/business'
    df_business = spark.read.json(input1,schema=schema.business_schema)
    res = df_business.filter(df_business.stars == 5).cache()
    
    num_state = res.groupBy(res['state']).agg(functions.count('business_id').alias('num_state')).withColumnRenamed('state','state1')
    num_city =  res.groupBy(res['city']).agg(functions.count('business_id').alias('num_city')).withColumnRenamed('city','city1')
    res = res.join(num_state, res.state == num_state.state1, 'inner').join(num_city,num_city.city1 == res.city, 'inner')
    res = res.select('city', 'num_city', 'state', 'num_state', 'longitude','latitude').distinct()
    res.write.json(output,mode ='append')

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)

    spark = SparkSession(sc)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    # business data
    input1 = sys.argv[1]
    # output
    output = sys.argv[2]
    main(input1, output)