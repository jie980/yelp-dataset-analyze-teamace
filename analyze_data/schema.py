import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import functions, types
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
business_schema = types.StructType([
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
                          .add('HasTV', types.StringType())
                          .add('Alcohol', types.StringType())
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

tips_schema = types.StructType([
    types.StructField('user_id', types.StringType()),
    types.StructField('business_id', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('compliment_count', types.IntegerType()),
])

user_schema = types.StructType([
    types.StructField('user_id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('review_count', types.StringType()),
    types.StructField('yelping_since', types.DateType()),
    types.StructField('useful', types.FloatType()),
    types.StructField('funny', types.FloatType()),
    types.StructField('cool', types.FloatType()),
    types.StructField('elite', types.StringType()),
    types.StructField('fans', types.FloatType()),
    types.StructField('average_stars', types.FloatType()),
    types.StructField('num_elite', types.FloatType()),
])