import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def main(input1, input2, output):
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
                          .add('BikeParking', types.BooleanType())
                          .add('WiFi', types.StringType())
                          .add('HasTV', types.BooleanType()).add('Alcohol', types.StringType())
                          .add('BusinessAcceptsCreditCards', types.BooleanType())
                          .add('RestaurantsReservations', types.BooleanType())
                          .add('RestaurantsPriceRange2', types.StringType())
                          .add('RestaurantsGoodForGroups', types.StringType())
                          .add('OutdoorSeating', types.BooleanType())
                          .add('DogsAllowed', types.BooleanType())
                          .add('NoiseLevel', types.BooleanType())
                          .add('GoodForMeal', types.StructType())
                          .add('RestaurantsTakeOut', types.BooleanType())
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

    business = spark.read.json(input1, schema=business_schema)
    review = spark.read.json(input2, schema=reviews_schema)
    restaurants = review.join(business, review['business_id'] == business['business_id']) \
        .select(review['business_id'], business['name'], business['attributes'], business['stars'], review['date'], review['useful']).cache()
    high_rate_restaurant = restaurants.filter(restaurants['stars'] >= 1).cache()

    # find high rate restaurants in 2008
    restaurants_2008 = high_rate_restaurant.filter(restaurants['date'].between('2008-01-01', '2009-01-01'))
    restaurants_2008 = restaurants_2008.select('*').groupBy('name')\
        .agg(functions.count('business_id').alias('order_numbers'), functions.avg('stars').alias('average_stars'), functions.sum('useful').alias('userful_nums'))
    # restaurants_2008 = restaurants_2008.select('name').distinct()


    # find high rate restaurants in 2012
    restaurants_2012 = high_rate_restaurant.filter(restaurants['date'].between('2012-01-01', '2013-01-01'))
    restaurants_2012 = restaurants_2012.select('*').groupBy('name') \
        .agg(functions.count('business_id').alias('order_numbers'), functions.avg('stars').alias('average_stars'), functions.sum('useful').alias('userful_nums'))
    # restaurants_2012 = restaurants_2012.select('name','stars').distinct()


    # find high rate restaurants in 2016
    restaurants_2016 = high_rate_restaurant.filter(restaurants['date'].between('2016-01-01', '2016-01-01'))
    restaurants_2016 = restaurants_2016.select('*').groupBy('name') \
        .agg(functions.count('business_id').alias('order_numbers'), functions.avg('stars').alias('average_stars'), functions.sum('useful').alias('userful_nums'))
    # restaurants_2016 = restaurants_2016.select('name').distinct()


    # find high rate restaurants in 2020
    restaurants_2020 = high_rate_restaurant.filter(restaurants['date'].between('2020-01-01', '2021-01-01'))
    restaurants_2020 = restaurants_2020.select('*').groupBy('name') \
        .agg(functions.count('business_id').alias('order_numbers'), functions.avg('stars').alias('average_stars'),  functions.sum('useful').alias('userful_nums'))
    #restaurants_2020 = restaurants_2020.select('name').distinct()

    print("total  numbers of Restaurants from 2008 to 2020: ", restaurants_2008.count(), restaurants_2012.count(), restaurants_2016.count(), restaurants_2020.count())

    restaurants_2008.write.json(output + '/2008', mode='overwrite')
    restaurants_2012.write.json(output + '/2012', mode='overwrite')
    restaurants_2016.write.json(output + '/2016', mode='overwrite')
    restaurants_2020.write.json(output + '/2020', mode='overwrite')
    #
    # df = restaurants_2008.join(restaurants_2020, restaurants_2008['name'] == restaurants_2020['name'])
    # print(df.count())
    order_2008 = restaurants_2008.groupBy('order_numbers').sum().collect()[0][1]
    order_2012 = restaurants_2012.groupBy('order_numbers').sum().collect()[0][1]
    order_2016 = restaurants_2016.groupBy('order_numbers').sum().collect()[0][1]
    order_2020 = restaurants_2020.groupBy('order_numbers').sum().collect()[0][1]
    print("total order numbers from 2008 to 2020: ", order_2008, order_2012, order_2016, order_2020)

    high_rate_2008 = restaurants_2008.filter(restaurants_2008['average_stars'] >= 4)
    high_rate_2012 = restaurants_2012.filter(restaurants_2012['average_stars'] >= 4)
    high_rate_2016 = restaurants_2016.filter(restaurants_2016['average_stars'] >= 4)
    high_rate_2020 = restaurants_2020.filter(restaurants_2020['average_stars'] >= 4)
    print("total high rate restaurants from 2008 to 2020: ", high_rate_2008.count(), high_rate_2012.count(), high_rate_2016.count(), high_rate_2020.count())

    df = restaurants_2008.join(restaurants_2020, restaurants_2008['name'] == restaurants_2020['name'], 'inner')
    print("How many restaurant survived from 2008 to 2020: ", df.count())

if __name__ == '__main__':
    input1 = sys.argv[1]
    input2 = sys.argv[2]
    output = sys.argv[3]
    spark = SparkSession.builder.appName('yelp').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input1, input2, output)
