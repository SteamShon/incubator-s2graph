package org.apache.s2graph.s2jobs.task.custom.process

import java.io.File

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}
import org.scalatest.FunSuite

import scala.io.Source

class ALSAnnoyBuildProcessTest extends FunSuite with DataFrameSuiteBase {

  test("ALSAnnoyBuildProcess") {
    import spark.sqlContext.implicits._
    val ratingPath = "/Users/shon/Downloads/ml-100k/u.data"
    val ratings = Source.fromFile(new File(ratingPath)).getLines().toSeq.map { line =>
      val tokens = line.split("\t")
      (tokens(0).toInt, tokens(1).toInt, tokens(2).toFloat)
    }.toDF("userId", "movieId", "rating")

    val conf = TaskConf("test", "test", Nil, Map("outputPath" -> "./annoy_test"))
    ALSAnnoyBuildProcess.buildAnnoyIndex(spark, conf, ratings)
  }
}
