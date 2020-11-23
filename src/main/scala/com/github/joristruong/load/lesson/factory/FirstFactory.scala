package com.github.joristruong.load.lesson.factory

import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.connector.Connector
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

/**
 * This `FirstFactory` is similar to the previous `WriteFactory`.
 * Instead of writing the result, we are going to pass it in the `get()` function.
 */
class FirstFactory extends Factory[DataFrame] with HasSparkSession {

  @Delivery(id = "date")
  val date: String = ""
  @Delivery(id = "testObject")
  val testObjectConnector: Connector = Connector.empty

  var testObject: DataFrame = spark.emptyDataFrame

  var result: DataFrame = spark.emptyDataFrame

  override def read(): FirstFactory.this.type = {
    testObject = testObjectConnector.read()

    this
  }

  override def process(): FirstFactory.this.type = {
    result = testObject
      .withColumn("date", lit(date))

    this
  }

  override def write(): FirstFactory.this.type = this

  /**
   * The `get()` function is the fourth executed function in a `Factory`, after `read()`, `process()` and `write()`.
   * Remember that the type of the output is defined at the start of the `Factory`, when specifying the parent class.
   * As you can see below, you simply return the variable storing the result.
   * This result is then injected in the `Pipeline` as a `Deliverable`. The other `Factory` can then ingest it.
   * Now let's head over to `SecondFactory`.
   */
  override def get(): DataFrame = result
}
