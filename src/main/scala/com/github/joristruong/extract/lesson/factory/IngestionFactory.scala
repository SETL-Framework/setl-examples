package com.github.joristruong.extract.lesson.factory

import com.github.joristruong.utils.TestObject
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.connector.Connector
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * A `SETL Factory` contains 4 main functions: `read()`, `process()`, `write()` and `get()`.
 * These functions will be executed in this order.
 * These 4 functions are the core of your ETL process. This is where you will write your classic `Spark` code of data transformation.
 *
 * You can see that `IngestionFactory` is a child class of `Factory[DataFrame]`.
 * This simply means that the output of this data transformation must be a `DataFrame`.
 * `IngestionFactory` also has the trait `HasSparkSession`.
 * It allows you to access the `SparkSession` easily.
 * Usually, we use it simply to import `spark.implicits`.
 */
class IngestionFactory extends Factory[DataFrame] with HasSparkSession {

  import spark.implicits._

  /**
   * The structure of a `SETL Factory` starts here, with the `@Delivery` annotation.
   * This annotation is the way `SETL` ingest the corresponding registered data.
   * If you look at `App.scala` where this `Factory` is called (line 113), the associated `Setl` object has registered a
   * `Connector` with id `testObjectConnector` (line109) and a `SparkRepository` with id `testObjectRepository` (line 110).
   *
   * Note that it is not mandatory to use a `deliveryId` in this case, because there is only one `Factory` with `TestObject` as object type.
   * You can try to remove the `deliveryId` when registering the `SparkRepository` and the `id` in the `@Delivery` annotation. The code will still run.
   * Same for the `Connector`.
   */
  @Delivery(id = "testObjectConnector")
  val testObjectConnector: Connector = Connector.empty
  @Delivery(id = "testObjectRepository")
  val testObjectRepository: SparkRepository[TestObject] = SparkRepository[TestObject]

  /**
   * With the `@Delivery` annotation, we retrieved a `Connector` and `SparkRepository`. These are data access layers.
   * To process the data, we have to retrieve the `DataFrame` of the `Connector` and the `Dataset` of the `SparkRepository`.
   * This is why we defined two `var`, one of type `DataFrame` and one of type `Dataset[TestObject]`. We will assign values to them during the `read()` function.
   * These `var` are accessible from all the 4 core functions. You will use them for your ETL process.
   */
  var testObjectOne: DataFrame = spark.emptyDataFrame
  var testObjectTwo: Dataset[TestObject] = spark.emptyDataset[TestObject]

  /**
   * The `read()` function is typically where you will do your data preprocessing.
   * Usually, we will simply assign values to our variables.
   */
  override def read(): IngestionFactory.this.type = {
    testObjectOne = testObjectConnector.read()
        .withColumnRenamed("value1", "valueOne") // Not needed, just for example
        .withColumnRenamed("value2", "valueTwo") // Not needed, just for example
    testObjectTwo = testObjectRepository.findAll()

    // Showing that ingestion worked correctly
    testObjectOne.show(false)
    testObjectTwo.show(false)

    this
  }

  /**
   * We will learn about the `process()` function during the **Transform** chapter.
   */
  override def process(): IngestionFactory.this.type = this

  /**
   * We will learn about the `write()` function during the **Load** chapter.
   */
  override def write(): IngestionFactory.this.type = this

  /**
   * We will learn about the `get()` function during the **Load** chapter.
   */
  override def get(): DataFrame = spark.emptyDataFrame
}
