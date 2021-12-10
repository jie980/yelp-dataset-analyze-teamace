import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, expr, when, avg, size,split
# add more functions as necessary

def main(inputs0, inputs1, inputs2, inputs3, output1,output2,output3):
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
        types.StructField('useful', types.IntegerType()),
        types.StructField('funny', types.IntegerType()),
        types.StructField('cool', types.IntegerType()),
        types.StructField('elite', types.StringType()),
        types.StructField('fans', types.IntegerType()),
        types.StructField('average_stars', types.FloatType()),
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
        types.StructField('attributes', types.StructType()
                          .add("RestaurantsTableService", types.StringType())
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
                          # .add('GoodForMeal', types.StructType()
                          #      .add('desert', types.BooleanType())
                          #      .add('breakfast', types.BooleanType())
                          #      .add('lunch', types.BooleanType())
                          #      .add('dinner', types.BooleanType())
                          #      .add('latenight', types.BooleanType()))
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


    #
    df = spark.read.json(inputs0, schema=business_schema).cache()

    df_v = df.filter(df.city == "Vancouver").filter(df.review_count > 50).filter(df.stars > 3.0)\
        .withColumn("total_star", df.stars * df.review_count)
    df_v = df_v.select("business_id", "name", "stars", "review_count", "total_star")\
        .orderBy(df_v.total_star.desc(), df_v.review_count.desc(), df_v.stars.desc()).limit(100)
    df_v.show(100)

    df_result = df_v.groupby(df.stars).agg(functions.count("stars"))

    df_result.write.json(output1, mode='overwrite')
# ------------------------------
    df_u = spark.read.json(inputs1, schema=user_schema)
    df_r = spark.read.json(inputs2, schema=reviews_schema).cache()
    df_t = spark.read.json(inputs3, schema=tips_schema).cache()
    df_r = df_r.withColumnRenamed("date", "r_date").withColumnRenamed("user_id","r_user_id").withColumnRenamed("business_id", "r_business_id")
    tips_review_condition = [df_t['user_id'] == df_r['r_user_id'], df_t['business_id'] == df_r['r_business_id']]
    tips_review = df_t.join(df_r, tips_review_condition, 'outer')
    tips_review_sync = tips_review.withColumn("t_r_sync", when(col("user_id") == col("r_user_id"), "true").otherwise("false"))


    tips_review_true = tips_review_sync.filter(tips_review_sync.t_r_sync == "true")
    df_u = df_u.withColumnRenamed("useful", "df_u_useful").withColumnRenamed("funny","df_u_funny").withColumnRenamed("cool", "df_u_cool")
    tips_review_true_user = tips_review_true.join(df_u, tips_review_true['r_user_id'] == df_u['user_id'], "inner").select("r_user_id", "df_u_useful","df_u_funny","df_u_cool","elite")
    tips_review_true_user = tips_review_true_user.distinct().select(avg(tips_review_true_user["df_u_useful"]).alias("useful")
                                                                    ,avg(tips_review_true_user["df_u_funny"]).alias("funny")
                                                                    ,avg(tips_review_true_user["df_u_cool"]).alias("cool")
                                                                    ,avg(size(split(col("elite"), ","))).alias("elite_count"))


    tips_review_false = tips_review_sync.filter(tips_review_sync.t_r_sync == "false")
    false_condition_1 = [tips_review_false['r_user_id'] == df_u['user_id']]
    false_condition_2 = [tips_review_false['user_id'] == df_u['user_id']]
    tips_review_false_user_1 = tips_review_false.join(df_u, false_condition_1,"inner").select("r_user_id", "df_u_useful", "df_u_funny", "df_u_cool","elite")
    tips_review_false_user_2 = tips_review_false.join(df_u, false_condition_2, "inner").select("r_user_id",
                                                                                               "df_u_useful",
                                                                                               "df_u_funny",
                                                                                               "df_u_cool", "elite")
    tips_review_false_user = tips_review_false_user_1.union(tips_review_false_user_2)
    tips_review_false_user = tips_review_false_user.distinct().select(avg(tips_review_false_user["df_u_useful"]).alias("useful")
                                                                    ,avg(tips_review_false_user["df_u_funny"]).alias("funny")
                                                                    ,avg(tips_review_false_user["df_u_cool"]).alias("cool")
                                                                    ,avg(size(split(col("elite"), ","))).alias("elite_count"))

    result = tips_review_true_user.union(tips_review_false_user)
    result.write.json(output2, mode='overwrite')
    # -------------------------
    tips_review_t = df_t.groupby("user_id", "business_id").agg(functions.count("date").alias("t_date"))
    tips_review_r = df_r.groupby("r_user_id", "r_business_id").agg(functions.count("r_date").alias("r_date"))
    t_r_compare_condition = [tips_review_t['user_id'] == tips_review_r['r_user_id'], tips_review_t['business_id'] == tips_review_r['r_business_id']]
    t_r_compare = tips_review_r.join(tips_review_t, t_r_compare_condition, 'inner')
    t_r_compare = t_r_compare.withColumn("t_r_state", when( col("t_date") == col("r_date"), "t_equal_r").when(col("t_date") > col("r_date"), "t_lgt_r").otherwise("t_let_r"))
    t_r_compare_ratio = t_r_compare.groupby("t_r_state").count()
    t_r_compare_ratio.write.json(output3, mode='overwrite')


if __name__ == '__main__':

    inputs0 = sys.argv[1]
    inputs1 = sys.argv[2]
    inputs2 = sys.argv[3]
    inputs3 = sys.argv[4]
    output1 = sys.argv[5]
    output2 = sys.argv[6]
    output3 = sys.argv[7]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs0, inputs1, inputs2, inputs3, output1, output2, output3)