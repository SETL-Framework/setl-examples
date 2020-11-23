package com.github.joristruong.load.lesson.factory

import com.github.joristruong.utils.TestObject
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.lit

class SecondFactoryBis extends Factory[DataFrame] with HasSparkSession {

  import spark.implicits._

  @Delivery(id = "date")
  val date: String = ""

  /**
   * The result of `FirstFactoryBis` is a `Dataset[TestObject]`.
   * Compared to `SecondFactory`, we did not need to use the `producer` in the `@Delivery` annotation.
   * This is because the `Pipeline` can infer on the data, and the only `Dataset[TestObject]` that it found is produced by `FirstFactoryBis`.
   * There is no need to specify it.
   *
   * This is the same mechanism that explains why a `Connector` needs a `deliveryId` to be retrieved, and not a `SparkRepository[T]` if there is only one of type T that is registered.
   */
  @Delivery
  val firstFactoryBisResult: Dataset[TestObject] = spark.emptyDataset

  var secondResult: DataFrame = spark.emptyDataFrame

  override def read(): SecondFactoryBis.this.type = this

  override def process(): SecondFactoryBis.this.type = {
    secondResult = firstFactoryBisResult
      .withColumn("secondDate", lit("date"))

    secondResult.show(false)

    this
  }

  override def write(): SecondFactoryBis.this.type = this

  override def get(): DataFrame = secondResult
}
