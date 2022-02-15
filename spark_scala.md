Learn about Spark Transform

https://towardsdatascience.com/dataframe-transform-spark-function-composition-eb8ec296c108



```scala
def ratingTransform(
    order_weight: Double,
    share_weight: Double,
    view_weight: Double,
    wishlist_weight: Double,
    click_weight: Double,
    view_cutoff: Int,
    spark: SparkSession
  )(ds: Dataset[TimeAggSchema]): DataFrame = {

    import spark.implicits._
    val linear_rating_cutoff = udf((v: Int, s: Int, o: Int, c: Int, w: Int) => {
      val w2 = if (w > 0) 1 else 0
      if (v > view_cutoff | (s > 0 | o > 0 | c > 0 | w > 0)) {
        o * order_weight + s * share_weight + v * view_weight + w2 * wishlist_weight + c * click_weight
      } else
        -1.0
    })
    ds.select(
        col("userId"),
        col("catalogId"),
        linear_rating_cutoff(
          col("views"),
          col("shares"),
          col("orders"),
          col("clicks"),
          col("wishlist")
        ).as("rating")
      )
      .filter("rating > -0.1")
  }
}

val ratingFunction =
      LinearRatingWithNonInteractedViewsCutoff.ratingTransform _

val trainingData =
      getAggregatedData(users, timeWindowForTrainingData, PBDUXWeights, spark)
      .transform(
        ratingFunction(
          appConfig.getDouble("order_weight"),
          appConfig.getDouble("share_weight"),
          appConfig.getDouble("view_weight"),
          appConfig.getDouble("wishlist_weight"),
          appConfig.getDouble("click_weight"),
          appConfig.getInt("view_cutoff"),
          spark
        )
      )
```

create case class

```scala
case class HyperParametersALS(
  rank: Int,
  numIterations: Int,
  alpha: Double,
  lambda: Double,
  setImplicitPrefs: Boolean,
  setNonNegative: Boolean,
  NumItemBlocks: Int,
  NumUserBlocks: Int
)

val hp = HyperParametersALS(
      appConfig.getInt("rank"),
      appConfig.getInt("numIterations"),
      appConfig.getDouble("alpha"),
      appConfig.getDouble("lambda"),
      appConfig.getBoolean("setImplicitPrefs"),
      appConfig.getBoolean("setNonNegative"),
      appConfig.getInt("NumItemBlocks"),
      appConfig.getInt("NumUserBlocks")
    )
```

Usage of 
* Equal operator
* filter command
* drop duplicates
* selectExpr

```scala
val primary_users =
      user_data
      .filter($"mod" === num.toInt)
      .select("user_id")

val filtered_df = df.filter("userId is not null and catalogId is not null")

val users =
      primary_users
      .filter("user_id is not null")
      
val originalUserId =
      users.selectExpr("user_id as original_user_id")
      .dropDuplicates("original_user_id")
```

zipwithindex example

```scala
val userIdMap = originalUserId
    .rdd
    .zipWithIndex()
    .map(t => (t._1.get(0).toString, t._2))
    .toDF("original_user_id", "index")
    .withColumn("temp_user_id", expr("cast(index as int)"))
    .drop("index")
    .repartition(400)
```

cast
```scala
df.selectExpr(
      "user_id as userId",
      "cast(catalog_id as Int) as catalogId",
      "cast(orders as Int) as orders",
      "cast(shares as Int) as shares",
      "cast(views as float) as views",
      "cast(clicks as Int) as clicks",
      "cast(wishlist as Int) as wishlist"
    )
```


```scala
views
    .join(orders, Seq("user_id", "catalog_id"), "full")
    .join(clicks, Seq("user_id", "catalog_id"), "full")
    .join(shares, Seq("user_id", "catalog_id"), "full")
    .join(wishlist, Seq("user_id", "catalog_id"), "full")
    .na
    .fill(0, Seq("orders", "shares", "wishlist", "clicks", "views"))
```

Min-Max Normalization
```pyspark
w = Window.partitionBy('group')
for c in cols_to_normalize:
    df = (df.withColumn('mini', F.min(c).over(w))
        .withColumn('maxi', F.max(c).over(w))
        .withColumn(c, ((F.col(c) - F.col('mini')) / (F.col('maxi') - F.col('mini'))))
        .drop('mini')
        .drop('maxi'))
```
