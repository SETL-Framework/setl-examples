package com.github.joristruong.transform.exercise

import com.github.joristruong.utils.Grade
import com.jcdecaux.setl.Setl
import com.jcdecaux.setl.config.ConfigLoader

/**
 * Learn about Transform processes with SETL
 */
object App {
  def main(args: Array[String]): Unit = {
    /**
     * In this exercise, we are going to practice about structuring a `SETL` project for transformation processes.
     * First, we are going to extract the data: `pokeGrades.csv` and `digiGrades.csv`, from `src/main/resources/`.
     * We are looking to looking to compute the mean score of each "poke" and then of each "digi".
     * After that, we can merge the two results. Let's get started.
     */

    /**
     * 0. Make sure you understand this. Take a look at the configuration file.
     */
    val setl: Setl = Setl.builder()
      .withDefaultConfigLoader("exercise/transform/transform.conf")
      .setSparkMaster("local[*]")
      .getOrCreate()

    /**
     * 1. Register pokeGradesRepository and digiGradesRepository into the entry point
     */

    /**
     * 2. Now, you need to write your `Factory`.
     */
    setl
      .newPipeline()
      .addStage[MeanGradeFactory]()
      .run()

  }
}
