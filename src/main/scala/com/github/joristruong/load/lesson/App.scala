package com.github.joristruong.load.lesson

import com.github.joristruong.load.lesson.factory.WriteFactory
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
  }
}
