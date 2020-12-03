package com.github.joristruong.load.exercise

import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.connector.Connector
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.{DataFrame, functions}

/**
 * 2.0 It is already done here, but always think about the type of your output.
 * You can of course think about it when you finished writing your `Factory`.
 * But to test it, make sure the output type correspond to the returned object in the `get()` function.
 */
class ExamsStatsFactory extends Factory[DataFrame] with HasSparkSession {

  /**
   * 2.5 Add your input Deliveries here.
   * You also need an output Delivery, where to write your output.
   */

  // Used to store the result
  var result: DataFrame = spark.emptyDataFrame

  // Use the `read()` function if necessary
  override def read(): ExamsStatsFactory.this.type = this

  /**
   * 2.6 Our goal is to compute the number of exams per year.
   * The input data is a `DataFrame` of 1 single column `date`.
   * Replace the `date` column by extracting the year only: it is the first 4 characters of the `date` column.
   * Then, you will have to count the number of `date`. Use the `groupBy()`, `agg()` and `count()` functions.
   * In our data, each year is duplicated 10 times. Indeed, for each exam, there are always 10 "poke" or 10 "digi". As a consequence, we need to divide the count by 10.
   * You should have by now the number of exams per year.
   */
  override def process(): ExamsStatsFactory.this.type = {
    result = ???

    result.show(false)

    this
  }

  /**
   * 2.7 Use the output Delivery you declared to save the result output.
   */
  override def write(): ExamsStatsFactory.this.type = {

    this
  }

  // We are returning the result; but we do not use it.
  override def get(): DataFrame = result
}
