package com.github.joristruong.extract.exercise

import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.connector.Connector
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.DataFrame

class CheckExtractFactory extends Factory[Int] with HasSparkSession {

//  @Delivery(id = "csvFile")
//  val csvFileConnector: Connector = Connector.empty
//  @Delivery(id = "jsonFile")
//  val jsonFileConnector: Connector = Connector.empty
//  @Delivery(id = "parquetFile")
//  val parquetFileConnector: Connector = Connector.empty
//  @Delivery(id = "excelFile")
//  val excelFileConnector: Connector = Connector.empty
//  @Delivery(id = "dynamoDBData")
//  val dynamoDBDataConnector: Connector = Connector.empty
//  @Delivery(id = "cassandraData")
//  val cassandraDataConnector: Connector = Connector.empty
//  @Delivery(id = "jdbcData")
//  val jdbcDataConnector: Connector = Connector.empty

//  var csvFile: DataFrame = spark.emptyDataFrame
//  var jsonFile: DataFrame = spark.emptyDataFrame
//  var parquetFile: DataFrame = spark.emptyDataFrame
//  var excelFile: DataFrame = spark.emptyDataFrame
//  var dynamoDBData: DataFrame = spark.emptyDataFrame
//  var cassandraData: DataFrame = spark.emptyDataFrame
//  var jdbcData: DataFrame = spark.emptyDataFrame

  override def read(): CheckExtractFactory.this.type = {
//    csvFile = csvFileConnector.read()
//    jsonFile = jsonFileConnector.read()
//    parquetFile = parquetFileConnector.read()
//    excelFile = excelFileConnector.read()
//    dynamoDBData = dynamoDBDataConnector.read()
//    cassandraData = cassandraDataConnector.read()
//    jdbcData = jdbcDataConnector.read()

//    assert(csvFile.count == 264)
//    assert(jsonFile.count == 179)
//    assert(parquetFile.count == 644)
//    assert(excelFile.count == 288)
//    assert(dynamoDBData.count == 5)
//    assert(cassandraData.count == 5)
//    assert(jdbcData.count == 3)

    this
  }

  override def process(): CheckExtractFactory.this.type = this

  override def write(): CheckExtractFactory.this.type = this

  override def get(): Int = 0
}
