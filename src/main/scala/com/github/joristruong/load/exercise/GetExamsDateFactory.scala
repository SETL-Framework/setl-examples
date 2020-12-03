package com.github.joristruong.load.exercise

import com.github.joristruong.utils.Grade
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * 2.0 It is already done here, but always think about the type of your output.
 * You can of course think about it when you finished writing your `Factory`.
 * But to test it, make sure the output type correspond to the returned object in the `get()` function.
 */
class GetExamsDateFactory extends Factory[DataFrame] with HasSparkSession {

  import spark.implicits._

  /**
   * 2.1 Add your input Deliveries here
   */

  // Used to store the result
  var examDates: DataFrame = spark.emptyDataFrame

  // Use the `read()` function if necessary
  override def read(): GetExamsDateFactory.this.type = {

    this
  }

  /**
   * 2.2 In the `process()` function, we are going to concatenate both the "poke" and the "digi" data.
   * Then, we will only keep the `date` column, as it is the only relevant column in this exercise.
   * As a reminder, we are looking to compute the number of exams per year.
   *
   * Hint: you can use the `coalesce()` function.
   */
  override def process(): GetExamsDateFactory.this.type = {
    examDates = ???

    this
  }

  // We will not write anything; think of it as an intermediate output which we do not want to write, but just to pass to the next `Factory`.
  override def write(): GetExamsDateFactory.this.type = this

  /**
   * 2.3 Here, you must return the correct output.
   */
  override def get(): DataFrame = ???
}
