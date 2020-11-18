package com.github.joristruong.extract

import com.github.joristruong.utils.{Grade, TestObject}
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
     * Using `setConnector()` and `setSparkRepository[T]()` make SETL register the corresponding data.
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

    /**
     * Most of the time, you will need to register multiple data sources.
     * Let's start with `Connector`. Note that it is perfectly possible to register multiple `Connector`.
     * However, and we will go through that later on, during the ingestion, there will be an issue if there are multiple `Connector`.
     * `Setl` has no way to differentiate one `Connector` from another.
     * You will need to set what is called a `deliveryId`.
     */
    val setl1: Setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    // /!\ This will work for data registration here but not for data ingestion later /!\
    setl1
      .setConnector("testObjectRepository")
      .setConnector("pokeGradesRepository")

    // Please get used to set a `deliveryId` when you register multiple `Connector`
    setl1
      .setConnector("testObjectRepository", deliveryId = "testObject")
      .setConnector("pokeGradesRepository", deliveryId = "grades")

    /**
     * Let's now look at how we can register multiple `SparRepository`.
     * If the `SparkRepository` you register all have different type, there will be no issue during the ingestion.
     * Indeed, `Setl` is capable of differentiating the upcoming data by inferring the object type.
     */
    val setl2: Setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    setl2
      .setSparkRepository[TestObject]("testObjectRepository")
      .setSparkRepository[Grade]("pokeGradesRepository")

    /**
     * However, if there are multiple `SparkRepository` with the same type, you *must* use a `deliveryId` for each of them.
     * Otherwise, there will be an error during the data ingestion.
     * This is the same reasoning as multiple `Connector`: there is no way to differentiate two `SparkRepository` of the same type.
     */
    val setl3: Setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    // /!\ This will work for data registration here but not for data ingestion later /!\
    setl3
      .setSparkRepository[Grade]("pokeGradesRepository")
      .setSparkRepository[Grade]("digiGradesRepository")

    // Please get used to set a `deliveryId` when you register multiple `SparkRepository` of same type
    setl3
      .setSparkRepository[Grade]("pokeGradesRepository", deliveryId = "pokeGrades")
      .setSparkRepository[Grade]("digiGradesRepository", deliveryId = "digiGrades")

  }
}
