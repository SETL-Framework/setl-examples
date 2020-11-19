package com.github.joristruong.extract

import com.github.joristruong.extract.factory.IngestionFactory
import com.github.joristruong.utils.{Grade, TestObject}
import com.jcdecaux.setl.Setl
import com.jcdecaux.setl.storage.connector.Connector

/**
 * Learn about Extract processes with SETL
 */
object App {
  def main(args: Array[String]): Unit = {
    val setl0: Setl = Setl.builder()
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
    setl0
      .setConnector("testObjectRepository")
      .setSparkRepository[TestObject]("testObjectRepository")

    /**
     * `connectorData` is a DataFrame and `sparkRepositoryData` is a Dataset[TestObject].
     */
    val connectorData = setl0.getConnector[Connector]("testObjectRepository").read()
    connectorData.show(false)
    val sparkRepositoryData = setl0.getSparkRepository[TestObject]("testObjectRepository").findAll()
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

    /**
     * Before deep diving into data ingestion, we first must learn about how `SETL` organizes an ETL process.
     * `SETL` uses `Pipeline` and `Stage` to organize workflows.
     * A `Pipeline` is where the whole ETL process will be done. The registered data are ingested inside a `Pipeline`, and all transformations and restitution will be done inside it.
     * A `Pipeline` is constituted of multiple `Stage`.
     * A `Stage` allows you to modulate your project. It can be constituted of multiple `Factory`. You can understand a `Factory` as a module of your ETL process.
     * So in order to "see" the data ingestion, we have to create a `Pipeline` and add a `Stage` to it.
     * As it may be a little bit theoretical, let's look at some examples.
     */
    val setl4: Setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    setl4
      .setConnector("testObjectRepository", deliveryId = "testObjectConnector")
      .setSparkRepository[TestObject]("testObjectRepository", deliveryId = "testObjectRepository")

    setl4
      .newPipeline() // Creation of a `Pipeline`.
      .addStage[IngestionFactory]() // Add a `Stage` composed of one `Factory`: `IngestionFactory`.
      // Before running the code, I invite you to go over `IngestionFactory` for more details. Feel free to remove the line comment below afterwards.
      //.run() // Run the `Pipeline` and execute what's inside it. In this case, the code inside `IngestionFactory` will be executed.

  }
}
