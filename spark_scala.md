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

```scala
case class userCatalog(userId: String, catalogs: Array[Catalog])
```

