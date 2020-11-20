package com.github.joristruong.transform.lesson.transformer

import com.jcdecaux.setl.transformation.Transformer
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

/**
 * A `Transformer` has two core methods:
 * `transform()` which is where the data transformation should happen.
 * `transformed` which is a getter to retrieve the result.
 *
 * Typically, we will also declare a variable in which we will assign the result of the transformation. In this case, `transformedData`.
 * The `transformed` getter returns this variable.
 * This is why in the corresponding `Factory`, the `transform()` method is called, before calling the `transformed` getter.
 */
class DateTransformer(testObject: DataFrame) extends Transformer[DataFrame] with HasSparkSession {
  private[this] var transformedData: DataFrame = spark.emptyDataFrame

  override def transformed: DataFrame = transformedData

  /**
   * This is where the data transformation happens.
   * It corresponds to your classic `Spark` process.
   *
   * Comparing to `ProcessFactory`, it is the first part of the data transformation.
   */
  override def transform(): DateTransformer.this.type = {
    transformedData = testObject
        .withColumn("date", lit("2020-11-20"))

    this
  }
}
