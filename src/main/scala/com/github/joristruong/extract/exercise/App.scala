package com.github.joristruong.extract.exercise

import com.jcdecaux.setl.Setl

/**
 * Learn about Extract processes with SETL
 */
object App {
  def main(args: Array[String]): Unit = {
    /**
     * Before doing this exercise, you will need to execute several Docker containers, in order to host a DynamoDB, Cassandra, and PostgreSQL instance.
     * Make sure you have Docker. In your terminal, change your directory to `exercise-environment` and execute `docker-compose up`.
     * Wait for the containers to be ready, and you will be set to complete the exercises on DynamoDB, Cassandra and PostgreSQL.
     *
     * In this exercise, we provided in `resources/exercise/extract/` some inputs files and a configuration file to be completed.
     * In the configuration file, the options need to be completed.
     * Check the documentation for hints, and open the files if needed.
     *
     * We are using `Connector`, but feel free to replace them by `SparkRepository` if you want to train yourself.
     */

    System.setProperty("aws.dynamodb.endpoint", "http://localhost:8000")

    val setl: Setl = Setl.builder()
      .withDefaultConfigLoader("exercise/extract/extract.conf")
      .setSparkMaster("local[*]")
      .getOrCreate()

    /**
     * Ingest a CSV file.
     * The provided file is located at `src/main/resources/exercise/extract/paris-wi-fi-service.csv`.
     * Please complete the appropriate configuration object and the `setConnector()` method.
     */
    //setl.setConnector(???)

    /**
     * Ingest an JSON file.
     * The provided file is located at `src/main/resources/exercise/extract/paris-notable-trees.json`.
     * Please complete the appropriate configuration object and the `setConnector()` method.
     */
    //setl.setConnector(???)

    /**
     * Ingest a Parquet file.
     * The provided file is located at `src/main/resources/exercise/extract/paris-public-toilet.parquet`.
     * Please complete the appropriate configuration object and the `setConnector()` method.
     */
    //setl.setConnector(???)

    /**
     * Ingest an Excel file.
     * The provided file is located at `src/main/resources/exercise/extract/paris-textile-containers.xlsx`.
     * Please complete the appropriate configuration object and the `setConnector()` method.
     */
    //setl.setConnector(???)

    /**
     * Ingest data from DynamoDB.
     * The data is located on the `us-east-1` region, and the name of the table is `orders_table`.
     * Please complete the appropriate configuration object and the `setConnector()` method.
     *
     * The endpoint is already configured to access http://localhost:8000.
     */
    //setl.setConnector(???)

    /**
     * Ingest data from Cassandra.
     * The data in located on the keyspace `mykeyspace` and the name of the table if `profiles`.
     * There is no `partitionKeyColumn` not `clusteringKeyColumn`.
     *
     * The endpoint is already configured to access http://localhost:9042.
     */
    //setl.setConnector(???)

    /**
     * Ingest data from JDBC.
     * The data is located in PostgreSQL. The hostname is `localhost` and the port is `5432`.
     * The name of the database is `postgres`, and the name of the table is `products`.
     * Use the username `postgres` and the password `postgres`.
     *
     * If you read the `SETL` documentation, you have to provide a JDBC driver.
     * To provide the PostgreSQL JDBC driver, head to https://jdbc.postgresql.org/download.html, download the driver, and make the JDBC library jar available to the project.
     * If you are using IntelliJ IDEA, right click on the jar and click on 'Add as Library'.
     */
    //setl.setConnector(???)

    /**
     * Create your own `Factory` or use the predefined one to check that the data has been correctly ingested
     */
    setl
      .newPipeline()
      .addStage[CheckExtractFactory]()
      .run()

    System.clearProperty("aws.dynamodb.endpoint")
  }
}
