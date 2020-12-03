package com.github.joristruong.load.exercise

import com.github.joristruong.utils.Grade
import com.jcdecaux.setl.Setl

/**
 * Learn about Load processes with SETL
 */
object App {
  def main(args: Array[String]): Unit = {
    /**
     * In this exercise, we are going to practice the Load processes with SETL, that is,
     * how to pass the result of a `Factory` to another `Factory`, and how to write the result of a `Factory`.
     * The files we are going to be working with are `pokeGrades.csv` and `digiGrades.csv` from `src/main/resources/`.
     *
     * We are going to find out how many exams there are per year.
     * To do that, a first `Factory` will "compute" all the dates from the data, and pass this result to the second `Factory`.
     * This second `Factory` ingest the result, extract the year of each date and count the number of exams per year.
     */

    /**
     * 0. Make sure you understand this. Take a look at the configuration file.
     */
    val setl: Setl = Setl.builder()
      .withDefaultConfigLoader("exercise/load/load.conf")
      .setSparkMaster("local[*]")
      .getOrCreate()

    /**
     * 1. Register pokeGradesRepository and digiGradesRepository into the entry point.
     * Also register a `Connector` where to write your output. Look at the prepared configuration file. You may change it if you want.
     */

    /**
     * 2. Now, you need to write two `Factory`: `GetExamsDateFactory` and `ExamsStatsFactory`.
     */
    setl
      .newPipeline()
      .addStage[GetExamsDateFactory]()
      .addStage[ExamsStatsFactory]()
      .run()
  }
}
