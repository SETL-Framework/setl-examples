package com.github.joristruong.transform.lesson

import com.github.joristruong.transform.lesson.factory.{ProcessFactory, ProcessFactoryWithTransformer}
import com.jcdecaux.setl.Setl

/**
 * Learn about Transform processes with SETL
 */
object App {
  def main(args: Array[String]): Unit = {
    /**
     * `SETL` Transform processes are quite easy if you are used to write ETL jobs with `Spark`:
     * this is where you will transfer the code you write with `Spark` into `SETL`.
     *
     * You should now what the below code do by now, so let's head over to `ProcessFactory`
     */
    val setl0: Setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    setl0
      .setConnector("testObjectRepository", deliveryId = "testObject")

    setl0
      .newPipeline()
      .addStage[ProcessFactory]()
      // Before running the code, I invite you to go over `ProcessFactory` for more details. Feel free to remove the line comment below afterwards.
      //.run()

    /**
     * You might not learn anything new for `SETL` for data transformations in itself, but `SETL` helps you to structure them.
     * We will now take a look about `SETL Transformer`. You already know about `Factory`. A `Factory` can contain multiple `Transformer`.
     * A `Transformer` is a piece of highly reusable code that represents one data transformation.
     * Let's look at how it works in `ProcessFactoryWithTransformer`.
     */
    val setl1: Setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    setl1
      .setConnector("testObjectRepository", deliveryId = "testObject")

    setl1
      .newPipeline()
      .addStage[ProcessFactoryWithTransformer]()
      // Before running the code, I invite you to go over `ProcessFactoryWithTransformer` for more details. Feel free to remove the line comment below afterwards.
      //.run()
  }
}
