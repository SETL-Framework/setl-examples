package com.github.joristruong.extract.lesson.factory

import com.github.joristruong.utils.TestObject
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.{DataFrame, Dataset}


class AutoLoadIngestionFactory extends Factory[DataFrame] with HasSparkSession {

  import spark.implicits._

  /**
   * *Inputs* are retrieved in the same way `Connector` or `SparkRepository` are retrieved:
   * the `@Delivery` annotation, and the `deliveryId` if necessary.
   */
  @Delivery
  val integer: Int = 0
  @Delivery(id = "ordered")
  val firstString: String = ""
  @Delivery(id = "reversed")
  val secondString: String = ""
  @Delivery
  val stringArray: Array[String] = Array()

  /**
   * The goal of this `AutoLoadIngestionFactory` is also to show the use of `autoLoad`.
   * In the previous `IngestionFactory`, we would set a `val` of type `SparkRepository` but also a `var` in which we assign the corresponding `Dataset` in the `read()` function.
   * With `autoLoad = true`, we can skip the first step and directly declare a `Dataset`.
   * The `Dataset` of the `SparkRepository` will be automatically assigned in it.
   * Note that there is no way to use the `findBy()` method to filter the data.
   *
   * `autoLoad` is available for `SparkRepository` only, and not for `Connector`.
   */
  @Delivery(id = "testObjectRepository", autoLoad = true)
  val testObject: Dataset[TestObject] = spark.emptyDataset[TestObject]

  override def read(): AutoLoadIngestionFactory.this.type = {
    // Showing that ingestion worked correctly
    testObject.show(false)

    // Showing that inputs work correctly
    println("integer: " + integer)
    println("ordered: " + firstString)
    println("reversed: " + secondString)
    println("array: " + stringArray.mkString("."))

    this
  }

  override def process(): AutoLoadIngestionFactory.this.type = this

  override def write(): AutoLoadIngestionFactory.this.type = this

  override def get(): DataFrame = spark.emptyDataFrame
}
