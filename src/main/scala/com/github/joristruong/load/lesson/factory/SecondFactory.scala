package com.github.joristruong.load.lesson.factory

import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.DataFrame

class SecondFactory extends Factory[DataFrame] with HasSparkSession {

  import spark.implicits._

  /**
   * In this `SecondFactory`, we want to retrieve the output produced by `FirstFactory`.
   * Noticed that we used the `producer` argument in the `@Delivery` annotation.
   */
  @Delivery(producer = classOf[FirstFactory])
  val firstFactoryResult: DataFrame = spark.emptyDataFrame

  var secondResult: DataFrame = spark.emptyDataFrame

  override def read(): SecondFactory.this.type = this

  override def process(): SecondFactory.this.type = {
    secondResult = firstFactoryResult
      .withColumn("secondDate", $"date")

    secondResult.show(false)

    this
  }

  override def write(): SecondFactory.this.type = this

  override def get(): DataFrame = secondResult
}
