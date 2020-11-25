package com.github.joristruong.extract.exercise

import com.jcdecaux.setl.Setl

/**
 * Learn about Extract processes with SETL
 */
object App {
  def main(args: Array[String]): Unit = {
    /**
     * In this exercise, we provided in `resources/exercise/extract/` some inputs files and a configuration file to be completed.
     * In the configuration file, the options need to be completed.
     * Check the documentation for hints, and open the files if needed.
     *
     * We are using `Connector`, but feel free to replace them by `SparkRepository` if you want to train yourself.
     */

    val setl: Setl = Setl.builder()
      .withDefaultConfigLoader("exercise/extract/extract.conf")
      .setSparkMaster("local[*]")
      .getOrCreate()

    /**
     * Ingest a CSV file.
     */
    setl.setConnector("csvFile", deliveryId = "csvFile")

    /**
     * Ingest an JSON file
     */
    setl.setConnector("jsonFile", deliveryId = "jsonFile")

    /**
     * Ingest a Parquet file
     */
    setl.setConnector("parquetFile", deliveryId = "parquetFile")

    /**
     * Ingest an Excel file
     */
    setl.setConnector("excelFile", deliveryId = "excelFile")

    /**
     * Ingest data from DynamoDB
     */
    //setl.setConnector("dynamoDBData", deliveryId = "dynamoDBData")

    /**
     * Ingest data from Cassandra
     */
    //setl.setConnector("cassandraData", deliveryId = "cassandraData")

    /**
     * Ingest data from JDBC
     */
    //setl.setConnector("jdbcData", deliveryId = "jdbcData")

    /**
     * Create your own `Factory` or use the predefined one to check that the data has been correctly ingested
     */
    setl
      .newPipeline()
      .addStage[CheckExtractFactory]()
      .run()
  }
}
