package com.github.joristruong.extract.exercise

import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.connector.Connector
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.DataFrame

class CheckExtractFactory extends Factory[Int] with HasSparkSession {

//  @Delivery(id = "???")
//  val csvFileConnector: Connector = Connector.empty
//  @Delivery(id = "???")
//  val jsonFileConnector: Connector = Connector.empty
//  @Delivery(id = "???")
//  val parquetFileConnector: Connector = Connector.empty
//  @Delivery(id = "???")
//  val excelFileConnector: Connector = Connector.empty
//  @Delivery(id = "???")
//  val dynamoDBDataConnector: Connector = Connector.empty
//  @Delivery(id = "???")
//  val cassandraDataConnector: Connector = Connector.empty
//  @Delivery(id = "???")
//  val jdbcDataConnector: Connector = Connector.empty
//  @Delivery(id = ???)
//  val deltaDataVersionZeroConnector: Connector = Connector.empty
//  @Delivery(id = ???)
//  val deltaDataVersionOneConnector: Connector = Connector.empty

//  var csvFile: DataFrame = spark.emptyDataFrame
//  var jsonFile: DataFrame = spark.emptyDataFrame
//  var parquetFile: DataFrame = spark.emptyDataFrame
//  var excelFile: DataFrame = spark.emptyDataFrame
//  var dynamoDBData: DataFrame = spark.emptyDataFrame
//  var cassandraData: DataFrame = spark.emptyDataFrame
//  var jdbcData: DataFrame = spark.emptyDataFrame
//  var deltaDataVersionZero: DataFrame = spark.emptyDataFrame
//  var deltaDataVersionOne: DataFrame = spark.emptyDataFrame

  override def read(): CheckExtractFactory.this.type = {
//    csvFile = csvFileConnector.read()
//    jsonFile = jsonFileConnector.read()
//    parquetFile = parquetFileConnector.read()
//    excelFile = excelFileConnector.read()
//    dynamoDBData = dynamoDBDataConnector.read()
//    cassandraData = cassandraDataConnector.read()
//    jdbcData = jdbcDataConnector.read()
//    deltaDataVersionZero = deltaDataVersionZeroConnector.read()
//    deltaDataVersionOne = deltaDataVersionOneConnector.read()

//    assert(csvFile.count == 264)
//    assert(jsonFile.count == 179)
//    assert(parquetFile.count == 644)
//    assert(excelFile.count == 288)
//    assert(dynamoDBData.count == 5)
//    assert(cassandraData.count == 5)
//    assert(jdbcData.count == 3)
//    assert(deltaDataVersionZero.count == 5)
//    assert(deltaDataVersionOne.count == 5)

    this
  }

  override def process(): CheckExtractFactory.this.type = this

  override def write(): CheckExtractFactory.this.type = this

  override def get(): Int = 0
}
