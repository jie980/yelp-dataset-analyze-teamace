# The script extract the categories and implements the word count to summaries the trending restaurant categories



# corresponding Redshift sql
# ```
# CREATE TABLE reviews (
    # categories_name CHAR(100),
    # number FLOAT
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

def words_once(line):
    text = json.loads(line)
    for i in text['categories'].split(', '):
        yield (i, 1)
    # yield(text['categories'])

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def get_value(kv):
    return kv[1]

def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs) #'./new data/business'
    words = text.flatMap(words_once)
    wordcount = words.reduceByKey(add)

    outdata = wordcount.sortBy(get_value,ascending=False)
    res = outdata.toDF().withColumnRenamed("_1", "categories").withColumnRenamed('_2','num')
    res.write.json(output,mode ='append')

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)

    spark = SparkSession(sc)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)