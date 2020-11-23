package com.github.joristruong.load.lesson

import com.github.joristruong.load.lesson.factory.{FirstFactory, FirstFactoryBis, SecondFactory, SecondFactoryBis, WriteFactory}
import com.jcdecaux.setl.Setl

/**
 * Learn about Load processes with SETL
 */
object App {
  def main(args: Array[String]): Unit = {
    /**
     * The Load processes with SETL correspond to two key ideas: writing the output, or passing the output.
     * Passing the output allows to pass the result of a `Factory` to another `Factory`, for example.
     * The latter is then using the result of a previous `Factory` as an input.
     */
    val setl0: Setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    /**
     * In order to write data, you need to register a `Connector` or a `SparkRepository`.
     * As you probably already know, if you want to write a `DataFrame`, register a `Connector`.
     * If you want to write a `Dataset`, register a `SparkRepository`.
     * Do not forget that you must create a configuration item in the configuration file.
     * There, you can specify the path of your output.
     */
    setl0
      .setConnector("testObjectRepository", deliveryId = "testObject")
      .setConnector("testObjectWriteRepository", deliveryId = "testObjectWrite")

    /**
     * Head over to `WriteFactory` to learn more about writing outputs.
     */
    setl0
      .newPipeline()
      .setInput[String]("2020-11-23", deliveryId = "date")
      .addStage[WriteFactory]()
      // Before running the code, I invite you to go over `WriteFactory` for more details. Feel free to remove the line comment below afterwards.
      //.run()

    /**
     * As SETL is organized with `Factory`, it is possible to pass the result of a `Factory` to another.
     * The result of a `Factory` can be of any type, it generally is a `DataFrame` or a `Dataset`. Remember that you specify the output type of a `Factory` when declaring it.
     * We are now going to ingest data and make some transformations in `FirstFactory`, then use the result in `SecondFactory`.
     */
    val setl1: Setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    setl1.setConnector("testObjectRepository", deliveryId = "testObject")

    /**
     * You can see in the `Pipeline` that `FirstFactory` is before `SecondFactory`.
     */
    setl1
      .newPipeline()
      .setInput[String]("2020-12-18", deliveryId = "date")
      .addStage[FirstFactory]()
      .addStage[SecondFactory]()
      // Before running the code, I invite you to go over `FirstFactory` and `SecondFactory` for more details. Feel free to remove the line comment below afterwards.
      //.run()

    /**
     * In the previous `Pipeline`, we retrieved the result of `FirstFactory` to use it in `SecondFactory`.
     * The result of `FirstFactory` was a `DataFrame`, and we needed to retrieve it in `SecondFactory` by using the `producer` argument in the `@Delivery` annotation.
     * In the following `Pipeline`, we are going to produce a `Dataset` from `FirstFactoryBis` and use it in `SecondFactoryBis`.
     */
    val setl2: Setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    setl2.setConnector("testObjectRepository", deliveryId = "testObject")

    setl2
      .newPipeline()
      .setInput[String]("2020-12-18", deliveryId = "date")
      .addStage[FirstFactoryBis]()
      .addStage[SecondFactoryBis]()
      // Before running the code, I invite you to go over `FirstFactory` and `SecondFactory` for more details. Feel free to remove the line comment below afterwards.
      .run()
  }
}
