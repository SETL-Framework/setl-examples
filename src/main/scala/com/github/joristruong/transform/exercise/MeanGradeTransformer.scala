package com.github.joristruong.transform.exercise

import com.github.joristruong.utils.Grade
import com.jcdecaux.setl.transformation.Transformer
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.{DataFrame, Dataset, functions}

/**
 * 2.2.1 You can see the this `Transformer` takes a parameter of type `Dataset[Grade]` and outputs an object of type `DataFrame`.
 */
class MeanGradeTransformer(grades: Dataset[Grade]) extends Transformer[DataFrame] with HasSparkSession {

  import spark.implicits._

  var transformedData: DataFrame = spark.emptyDataFrame

  override def transformed: DataFrame = transformedData

  /**
   * 2.2.2 This is where you will write your `Spark` data transformations.
   * Look at the `Grade` case class.
   * Remember the objective: return a `DataFrame` with columns `name` and `grade`, where `grade` is the mean grade of each `name`.
   */
  override def transform(): MeanGradeTransformer.this.type = {
    transformedData = ???

    this
  }
}
