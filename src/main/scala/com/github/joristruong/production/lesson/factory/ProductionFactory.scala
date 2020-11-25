package com.github.joristruong.production.lesson.factory

import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.connector.Connector
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.DataFrame

/**
 * The goal of this `Factory` is to verify that we have the same input file on local and production environment.
 */
class ProductionFactory extends Factory[Int] with HasSparkSession {

  @Delivery(id = "pokeGradesRepository")
  val pokeGradesRepository: Connector = Connector.empty
  @Delivery(id = "pokeGradesRepositoryProd")
  val pokeGradesRepositoryProd: Connector = Connector.empty

  var pokeGrades: DataFrame = spark.emptyDataFrame
  var pokeGradesProd: DataFrame = spark.emptyDataFrame

  override def read(): ProductionFactory.this.type = {
    pokeGrades = pokeGradesRepository.read()
    pokeGradesProd = pokeGradesRepositoryProd.read()

    assert(pokeGrades.except(pokeGradesProd).count == 0)
    assert(pokeGradesProd.except(pokeGrades).count == 0)

    this
  }

  override def process(): ProductionFactory.this.type = this

  override def write(): ProductionFactory.this.type = this

  override def get(): Int = 0
}
