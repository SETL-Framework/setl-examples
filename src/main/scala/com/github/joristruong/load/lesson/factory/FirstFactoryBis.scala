package com.github.joristruong.load.lesson.factory

import com.github.joristruong.utils.TestObject
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.connector.Connector
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{concat, lit}

class FirstFactoryBis extends Factory[Dataset[TestObject]] with HasSparkSession {

  import spark.implicits._

  @Delivery(id = "testObject")
  val testObjectConnector: Connector = Connector.empty

  var testObject: DataFrame = spark.emptyDataFrame

  var result: Dataset[TestObject] = spark.emptyDataset[TestObject]

  override def read(): FirstFactoryBis.this.type = {
    testObject = testObjectConnector.read()

    this
  }

  override def process(): FirstFactoryBis.this.type = {
    result = testObject
      .withColumn("value1", concat($"value1", lit("42")))
      .as[TestObject]

    this
  }

  override def write(): FirstFactoryBis.this.type = this

  /**
   * `result` is a variable of type `Dataset[TestObject]`.
   * This `Dataset` is injected into the `Pipeline`.
   * Let's head over to `SecondFactoryBis` to see how it is ingested.
   */
  override def get(): Dataset[TestObject] = result
}
