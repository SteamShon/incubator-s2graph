package org.apache.s2graph.s2jobs.task.custom.process

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import annoy4s.{Angular, Annoy, Euclidean}
import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, SparkSession}

object ALSAnnoyBuildProcess {
  def buildAnnoyIndex(ss: SparkSession,
                      conf: TaskConf,
                      dataFrame: DataFrame): Unit = {
    import ss.sqlContext.implicits._
    val outputPath = conf.options("outputPath")

    val als = new ALS()
      .setRank(10)
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(dataFrame)
    val lines = model.itemFactors.map { row =>
      val id = row.getAs[Int]("id")
      val vector = row.getAs[Seq[Float]]("features")
      (Seq(id) ++ vector).mkString(" ")
    }.collect()

    def writeToFile(fileName: String)(lines: Seq[String]): Unit = {
      val writer = new PrintWriter(fileName)
      lines.foreach(line => writer.write(line + "\n"))
      writer.close
    }

    writeToFile(s"$outputPath/input_vectors")(lines)
    Annoy.create[Int](s"$outputPath/input_vectors", 10, outputDir = s"$outputPath/annoy_result/", Angular)
  }
}
class ALSAnnoyBuildProcess(conf: TaskConf) extends org.apache.s2graph.s2jobs.task.Process(conf) {
  override def execute(ss: SparkSession, inputMap: Map[String, DataFrame]): DataFrame = ???

  override def mandatoryOptions: Set[String] = Set("outputPath")


}
