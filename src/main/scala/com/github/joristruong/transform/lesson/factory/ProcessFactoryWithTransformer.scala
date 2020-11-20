package com.github.joristruong.transform.lesson.factory

import com.github.joristruong.transform.lesson.transformer.{DateTransformer, RenameTransformer}
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.connector.Connector
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.DataFrame

/**
 * If you compare this `Factory` with the previous `ProcessFactory`, it does the same job.
 * However, the workflow is more structured. Head over to the `process()` function.
 */
class ProcessFactoryWithTransformer extends Factory[DataFrame] with HasSparkSession {

  @Delivery(id = "testObject")
  val testObjectConnector: Connector = Connector.empty

  var testObject: DataFrame = spark.emptyDataFrame

  var result: DataFrame = spark.emptyDataFrame

  override def read(): ProcessFactoryWithTransformer.this.type = {
    testObject = testObjectConnector.read()

    this
  }

  /**
   * The data transformation will be done in `SETL Transformer`.
   * This allows to make to code highly reusable and add a lot more structure to it.
   *
   * To do that, we are calling the first `Transformer` by passing our input `DataFrame`.
   * The `transform()` method is then called, and the result is retrieved with the `transformed` getter.
   * The second data transformation is done with `RenameTransformer`, and the result is assigned to our `result` variable.
   * Feel free to head over at the `Transformer` for more details.
   */
  override def process(): ProcessFactoryWithTransformer.this.type = {
    val testObjectDate = new DateTransformer(testObject).transform().transformed
    result = new RenameTransformer(testObjectDate).transform().transformed

    // Showing the data transformation worked correctly
    result.show(false)

    this
  }

  /**
   * We will learn about the `write()` function during the **Load** chapter.
   */
  override def write(): ProcessFactoryWithTransformer.this.type = this

  /**
   * We will learn about the `get()` function during the **Load** chapter.
   */
  override def get(): DataFrame = spark.emptyDataFrame
}
