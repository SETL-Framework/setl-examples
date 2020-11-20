package com.github.joristruong.load.lesson.factory

import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.connector.Connector
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class WriteFactory extends Factory[DataFrame] with HasSparkSession {

  /**
   * Note that in the `Deliveries`, there is one with the ID `testObjectWrite`.
   * It has been previously registered in the `Pipeline`. We are retrieving it, but using it as a way to write our output.
   */
  @Delivery(id = "date")
  val date: String = ""
  @Delivery(id = "testObject")
  val testObjectConnector: Connector = Connector.empty
  @Delivery(id = "testObjectWrite")
  val testObjectWriteConnector: Connector = Connector.empty

  var testObject: DataFrame = spark.emptyDataFrame

  var result: DataFrame = spark.emptyDataFrame

  /**
   * We a assigning the `DataFrame` of our input `Connector` in a variable.
   */
  override def read(): WriteFactory.this.type = {
    testObject = testObjectConnector.read()

    this
  }

  /**
   * Some data transformations are done here.
   */
  override def process(): WriteFactory.this.type = {
    result = testObject
        .withColumn("date", lit(date))

    this
  }

  /**
   * The `write()` function is the third executed function in a `Factory`, after `read()` and `process()`.
   * The idea is to call the `write()` method of a `Connector` or a `SparkRepository`, and pass the result `DataFrame` or `Dataset` as argument.
   * `SETL` will automatically read the configuration item; storage type, path and options, and write the result there.
   */
  override def write(): WriteFactory.this.type = {
    testObjectWriteConnector.write(result.coalesce(1))

    this
  }

  override def get(): DataFrame = spark.emptyDataFrame
}
