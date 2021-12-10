import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def main(inputs, output):
    comments_schema = types.StructType([
        types.StructField('business_id', types.StringType()),
        types.StructField('name', types.StringType()),
        types.StructField('address', types.StringType()),
        types.StructField('city', types.StringType()),
        types.StructField('state', types.StringType()),
        types.StructField('postal_code', types.LongType()),
        types.StructField('latitude', types.FloatType()),
        types.StructField('longitude', types.FloatType()),
        types.StructField('stars', types.FloatType()),
        types.StructField('review_count', types.IntegerType()),
        types.StructField('is_open', types.IntegerType()),
        types.StructField('attributes', types.StructType().add("RestaurantsTableService", types.StringType())
                          .add('BikeParking', types.StringType())
                          .add('WiFi', types.StringType())
                          .add('HasTV', types.BooleanType()).add('Alcohol', types.StringType())
                          .add('BusinessAcceptsCreditCards', types.StringType())
                          .add('RestaurantsReservations', types.StringType())
                          .add('RestaurantsPriceRange2', types.StringType())
                          .add('RestaurantsGoodForGroups', types.StringType())
                          .add('OutdoorSeating', types.StringType())
                          .add('DogsAllowed', types.StringType())
                          .add('NoiseLevel', types.StringType())
                          .add('RestaurantsTakeOut', types.StringType())
                          .add('RestaurantsDelivery', types.StringType())
                          .add('RestaurantsAttire', types.StringType())),
        types.StructField('categories', types.StringType()),
        types.StructField('hours', types.StructType().add('Monday', types.StringType())
                          .add('Tuesday', types.StringType())
                          .add('Wednesday', types.StringType())
                          .add('Thursday', types.StringType())
                          .add('Friday', types.StringType())
                          .add('Saturday', types.StringType())
                          .add('Sunday', types.StringType()))
    ])

    comments = spark.read.json(inputs, schema=comments_schema)
    print(comments.count())
    new_df = comments.select('*')
    print(new_df.show())
    df = new_df.filter((comments['stars'].isNotNull()) & (comments['categories'].isNotNull())
                             & (comments['state'].isNotNull()) )
    print(df.count())
    print(df.show())
    df.write.json(output, mode='overwrite')



if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('yelp').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)