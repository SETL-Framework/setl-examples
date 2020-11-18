package com.github.joristruong.extract

import com.github.joristruong.utils.TestObject
import com.jcdecaux.setl.Setl
import com.jcdecaux.setl.storage.connector.Connector

/**
 * Learn about Extract processes with SETL
 */
object App {
  def main(args: Array[String]): Unit = {
    val setl: Setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    /**
     * SETL supports two types of data accessors: Connector and SparkRepository.
     * A Connector is a non-typed abstraction of data access layer (DAL). For simplicity, you can understand it to as a Spark DataFrame.
     * A SparkRepository is a typed abstraction data access layer (DAL). For simplicity, you can understand it as a Spark Dataset.
     */

    /**
     * Using `setConnector()` and `setSparkRepository[T]()` make SETL ingest the corresponding data.
     * The provided argument is a string that must be found at the configuration file.
     * Multiple storage types are supported in SETL. We will go through each of them later on.
     * Make sure you understand the data object `testObjectRepository` that we defined in the configuration file.
     * For CSV files, SETL is using Apache Spark API. The options you see in the object (except for `storage` of course) correspond to the options used when reading a CSV file with Apache Spark.
     */
    setl
      .setConnector("testObjectRepository")
      .setSparkRepository[TestObject]("testObjectRepository")

    /**
     * `connectorData` is a DataFrame and `sparkRepositoryData` is a Dataset[TestObject].
     */
    val connectorData = setl.getConnector[Connector]("testObjectRepository").read()
    connectorData.show(false)
    val sparkRepositoryData = setl.getSparkRepository[TestObject]("testObjectRepository").findAll()
    sparkRepositoryData.show(false)
  }
}
