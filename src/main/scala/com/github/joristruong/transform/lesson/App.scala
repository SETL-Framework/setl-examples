package com.github.joristruong.transform.lesson

import com.github.joristruong.transform.lesson.factory.IngestionFactory
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
     * You should now what the below code do by now, so let's head over to `IngestionFactory`
     */
    val setl0: Setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    setl0
      .setConnector("testObjectRepository", deliveryId = "testObject")

    setl0
      .newPipeline()
      .addStage[IngestionFactory]()
      // Before running the code, I invite you to go over `IngestionFactory` for more details. Feel free to remove the line comment below afterwards.
      //.run()
  }
}
