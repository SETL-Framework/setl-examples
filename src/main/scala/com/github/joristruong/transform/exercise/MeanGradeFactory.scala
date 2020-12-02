package com.github.joristruong.transform.exercise

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
class MeanGradeFactory extends Factory[DataFrame] with HasSparkSession {

  import spark.implicits._

  /**
   * 2.1 Add your input Deliveries here
   */

  // Used to store the result
  var result: DataFrame = spark.emptyDataFrame

  // Use the `read()` function if necessary
  override def read(): MeanGradeFactory.this.type = {

    this
  }

  /**
   * 2.2 In the `process()` function, we are going to call a `Transformer` for each of our input `Delivery`.
   * This `Transformer` takes a `Dataset[Grade]` as a parameter, and will return a `DataFrame` with columns `name` and `grade`, where `grade` is the mean grade.
   * Write `MeanGradeTransformer`.
   */
  override def process(): MeanGradeFactory.this.type = {
    // Call MeanGradeTransformer to compute the mean grade on pokeGrades

    // Call MeanGradeTransformer to compute the mean grade on digiGrades

    /**
     * 2.3 Combine the two `Dataset` and store in `result` variable
     */
    result = ???

    /**
     * 2.4 Verification
     */
    result.show(false)
    assert(result.count == 20)

    this
  }

  override def write(): MeanGradeFactory.this.type = this

  override def get(): DataFrame = spark.emptyDataFrame
}
