import sys

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from afinn import Afinn
from pyspark.sql import SparkSession, functions, types
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import Row


def main(input1):
    reviews_schema = types.StructType([
        types.StructField('review_id', types.StringType()),
        types.StructField('user_id', types.StringType()),
        types.StructField('business_id', types.StringType()),
        types.StructField('stars', types.FloatType()),
        types.StructField('useful', types.FloatType()),
        types.StructField('funny', types.FloatType()),
        types.StructField('cool', types.FloatType()),
        types.StructField('text', types.StringType()),
        types.StructField('date', types.DateType()),
    ])

    review = spark.read.json(input1, schema=reviews_schema).select('review_id', 'stars', 'text').limit(10000)
    review.show(5)
    review.printSchema()

    # stage 1: tokenize the review text
    stage_1 = RegexTokenizer(inputCol='text', outputCol='tokens', pattern='\\W')
    # stage 2: remove the stop words
    stage_2 = StopWordsRemover(inputCol='tokens', outputCol='filtered_words')
    # stage 3: create a word vector of the size 100
    stage_3 = Word2Vec(inputCol='filtered_words', outputCol='vector', vectorSize=100)
    # stage 4: Logistic Regression Model
    model = LogisticRegression(featuresCol='vector', labelCol='stars')

    # set pipeline
    pipeline = Pipeline(stages=[stage_1, stage_2, stage_3, model])

    # fit the pipeline model with the training data
    print('\n\nFit the pipeline with the training data.......\n')
    pipelineFit = pipeline.fit(review)

    # compute sentiments of the received reviews
    def get_prediction(review_text):
        try:
            review_text = review_text.filter(lambda x: len(x) > 0)
            rowRdd = review_text.map(lambda w: Row(text=w))
            wordsDataFrame = spark.createDataFrame(rowRdd)
            pipelineFit.transform(wordsDataFrame).select('text', 'prediction').show()
        except:
            print('No data')

    print('\n\nModel Trained....Waiting for the Data!\n')

    ssc = StreamingContext(sc, batchDuration=3)
    lines = ssc.socketTextStream('localhost', 9009)
    words = lines.flatMap(lambda line: line.split('NEW_REVIEW'))
    words.foreachRDD(get_prediction)

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    input1 = sys.argv[1]
    spark = SparkSession.builder.appName('yelp').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input1)
