package com.github.joristruong.transform.lesson.factory

import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.connector.Connector
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class IngestionFactory extends Factory[DataFrame] with HasSparkSession {

  import spark.implicits._

  @Delivery(id = "testObject")
  val testObjectConnector: Connector = Connector.empty

  var testObject: DataFrame = spark.emptyDataFrame

  /**
   * We are declaring one variable in which we will store the result of our data transformations.
   */
  var result: DataFrame = spark.emptyDataFrame

  override def read(): IngestionFactory.this.type = {
    testObject = testObjectConnector.read()

    // Showing that ingestion worked correctly
    testObject.show(false)

    this
  }

  /**
   * `process()` is the function that will be executed right after `read()`.
   * This is where you will write your classic Spark functions.
   */
  override def process(): IngestionFactory.this.type = {
    val testObjectDate = testObject.withColumn("date", lit("2020-11-20"))

    result = testObjectDate
      .withColumnRenamed("value1", "name")
      .withColumnRenamed("value2", "grade")

    // Showing the data transformation worked correctly
    result.show(false)

    this
  }

  /**
   * We will learn about the `write()` function during the **Load** chapter.
   */
  override def write(): IngestionFactory.this.type = this

  /**
   * We will learn about the `get()` function during the **Load** chapter.
   */
  override def get(): DataFrame = spark.emptyDataFrame
}
