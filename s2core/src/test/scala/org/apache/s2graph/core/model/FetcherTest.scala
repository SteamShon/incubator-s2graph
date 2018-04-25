package org.apache.s2graph.core.model

import com.typesafe.config.ConfigFactory
import org.apache.s2graph.core.Integrate.IntegrateCommon
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.schema.Label
import org.apache.s2graph.core.{Query, QueryParam}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

class FetcherTest extends IntegrateCommon{
  import TestUtil._
  import scala.collection.JavaConverters._

  test("MemoryModelFetcher") {
    // 1. create label.
    // 2. importLabel.
    // 3. fetch.
    val service = management.createService("s2graph", "localhost", "s2graph_htable", -1, None).get
    val serviceColumn =
      management.createServiceColumn("s2graph", "user", "string", Seq(Prop("age", "0", "int", true)))
    val labelName = "fetcher_test"
    val options = s"""{
                     |
      | "importer": {
                     | "${ModelManager.ImporterClassNameKey}": "identity"
                     | },
                     | "fetcher": {
                     |   "${ModelManager.FetcherClassNameKey}": "memory"
                     | }
                     |}""".stripMargin

    Label.findByName(labelName, useCache = false).foreach { label => Label.delete(label.id.get) }

    val label = management.createLabel(
      labelName,
      serviceColumn,
      serviceColumn,
      true,
      service.serviceName,
      Seq.empty[Index].asJava,
      Seq.empty[Prop].asJava,
      "strong",
      null,
      -1,
      "v3",
      "gz",
      options
    )
    val config = ConfigFactory.parseString(options)
    val importerFuture = graph.modelManager.importModel(label, config)(ExecutionContext.Implicits.global)
    Await.ready(importerFuture, Duration("60 seconds"))

    Thread.sleep(1000)

    val vertex = graph.elementBuilder.toVertex(service.serviceName, serviceColumn.columnName, "daewon")
    val queryParam = QueryParam(labelName = labelName)

    val query = Query.toQuery(srcVertices = Seq(vertex), queryParams = Seq(queryParam))
    val stepResult = Await.result(graph.getEdges(query), Duration("60 seconds"))

    stepResult.edgeWithScores.foreach { es =>
      println(es.edge)
    }
  }
  test("AnnoyModelFetcher") {

    val labelName = "annoy_model_fetcher_test"
    val hdfsConfDir = "/usr/local/Cellar/hadoop/2.7.3/libexec/etc/hadoop/"

    val REMOTE_INDEX_FILE = "/Users/shon/Downloads/test-index.tree"
    val LOCAL_INDEX_FILE = "./test-index.tree"
    val REMOTE_DICT_FILE = "/Users/shon/Downloads/test-index.dict"
    val LOCAL_DICT_FILE = "./test-index.dict"

    val service = management.createService("s2graph", "localhost", "s2graph_htable", -1, None).get
    val serviceColumn =
      management.createServiceColumn("s2graph", "user", "string", Seq(Prop("age", "0", "int", true)))

    //    val options = s"""{
    //                     | "importer": {
    //                     |   "${ModelManager.ImporterClassNameKey}": "hdfs",
    //                     |   "${HDFSImporter.HDFSConfDirKey}": "$hdfsConfDir",
    //                     |   "${HDFSImporter.PathsKey}": {
    //                     |     "${REMOTE_INDEX_FILE}": "${LOCAL_INDEX_FILE}",
    //                     |     "${REMOTE_DICT_FILE}": "${LOCAL_DICT_FILE}"
    //                     |   }
    //                     | },
    //                     | "fetcher": {
    //                     |   "${ModelManager.FetcherClassNameKey}": "annoy",
    //                     |   "${AnnoyModelFetcher.IndexFilePathKey}": "${LOCAL_INDEX_FILE}",
    //                     |   "${AnnoyModelFetcher.DictFilePathKey}": "${LOCAL_DICT_FILE}",
    //                     |   "${AnnoyModelFetcher.DimensionKey}": 10
    //                     | }
    //                     |}""".stripMargin
    val options = s"""{
                     | "importer": {
                     |   "${ModelManager.ImporterClassNameKey}": "identity"
                     | },
                     | "fetcher": {
                     |   "${ModelManager.FetcherClassNameKey}": "annoy",
                     |   "${AnnoyModelFetcher.IndexFilePathKey}": "${REMOTE_INDEX_FILE}",
                     |   "${AnnoyModelFetcher.DictFilePathKey}": "${REMOTE_DICT_FILE}",
                     |   "${AnnoyModelFetcher.DimensionKey}": 10
                     | }
                     |}""".stripMargin

    Label.findByName(labelName, useCache = false).foreach { label => Label.delete(label.id.get) }

    val label = management.createLabel(
      labelName,
      serviceColumn,
      serviceColumn,
      true,
      service.serviceName,
      Seq.empty[Index].asJava,
      Seq.empty[Prop].asJava,
      "strong",
      null,
      -1,
      "v3",
      "gz",
      options
    )
    val config = ConfigFactory.parseString(options)
    val importerFuture = graph.modelManager.importModel(label, config)(ExecutionContext.Implicits.global)
    Await.result(importerFuture, Duration("3 minutes"))

    Thread.sleep(10000)

    val vertex = graph.elementBuilder.toVertex(service.serviceName, serviceColumn.columnName, "0")
    val queryParam = QueryParam(labelName = labelName, limit = 5)

    val query = Query.toQuery(srcVertices = Seq(vertex), queryParams = Seq(queryParam))
    val stepResult = Await.result(graph.getEdges(query), Duration("60 seconds"))

    stepResult.edgeWithScores.foreach { es =>
      println(es.edge)
    }
  }
}
