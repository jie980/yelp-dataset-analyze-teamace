import sys

import numpy as np

import schema
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession
from pyspark.sql import functions, types
import pyspark.ml.regression as R
import pyspark.ml.feature as F
import pyspark.ml.evaluation as E
from pyspark.ml import Pipeline
import matplotlib.pyplot as plt
def main(input1,outputs):
    df_business = spark.read.json(input1, schema=schema.business_schema).cache()
    df_business =df_business.select('business_id','stars','state','attributes.BikeParking','attributes.RestaurantsPriceRange2','review_count','attributes.BusinessAcceptsCreditCards','attributes.WiFi','attributes.NoiseLevel')
    df_business = df_business.withColumn('RestaurantsPriceRange2',df_business['RestaurantsPriceRange2'].cast(types.IntegerType()))
    df_business = df_business.filter(df_business['BikeParking'].isNotNull())\
        .filter(df_business['RestaurantsPriceRange2'].isNotNull())\
        .filter(df_business['BusinessAcceptsCreditCards'].isNotNull())\
        .filter(df_business['WiFi'].isNotNull())\
        .filter(df_business['NoiseLevel'].isNotNull())
    print(df_business.count())

    df_business = df_business.withColumnRenamed('BikeParking','bikeparking')\
        .withColumnRenamed('RestaurantsPriceRange2','pricerange')\
        .withColumnRenamed('BusinessAcceptsCreditCards','acceptcreditcard')\
        .withColumnRenamed('WiFi','wifi')\
        .withColumnRenamed('Noiselevel','noiselevel')

    train, validation = df_business.randomSplit([0.75, 0.25])
    state_indexer = F.StringIndexer(inputCols=['state','bikeparking','acceptcreditcard','wifi','noiselevel'],outputCols=['indexed_state','indexed_bikeparking','indexed_acceptcreditcard','indexed_wifi','indexed_noiselevel'])
    onehot_encoder=F.OneHotEncoder(inputCols=['indexed_state'],outputCols=['onehot_state'])
    assembler = F.VectorAssembler(inputCols=['onehot_state','indexed_bikeparking','indexed_acceptcreditcard','pricerange','indexed_wifi','indexed_noiselevel'],outputCol='features')
    predicter = R.RandomForestRegressor(featuresCol='features',labelCol='stars')
    data_transfrom_pipline = Pipeline(stages=[state_indexer,onehot_encoder,assembler,predicter])
    model = data_transfrom_pipline.fit(train)
    prediction_train = model.transform(train)
    model2 = data_transfrom_pipline.fit(validation)
    print(model2.stages[-1].featureImportances)
    prediction_valid = model2.transform(validation)
    r2evaluator = E.RegressionEvaluator(predictionCol='prediction',labelCol='stars',metricName='r2')
    score_train =r2evaluator.evaluate(prediction_train)
    score_valid =r2evaluator.evaluate(prediction_valid)
    print('Training score for weather model:', score_train)
    print('Validation score for weather model:', score_valid)
    prediction_train.show()
    prediction_valid.write.json(outputs,mode='overwrite')
    ppv = prediction_valid.toPandas()
    plt.scatter(y=ppv['prediction'],x=ppv['stars'])
    plt.xlabel('actual stars')
    plt.ylabel('predicted stars')
    plt.show()
if __name__ == '__main__':
    input1 = sys.argv[1]
    outputs = sys.argv[2]
    spark = SparkSession.builder.appName('preprocess_reviews').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input1,outputs)