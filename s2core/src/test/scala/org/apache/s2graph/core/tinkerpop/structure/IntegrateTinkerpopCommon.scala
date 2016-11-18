//package org.apache.s2graph.core.tinkerpop.structure
//
//import com.typesafe.config.{ConfigFactory, Config}
//import org.apache.s2graph.core.{Management, Graph}
//import org.apache.s2graph.core.Integrate.IntegrateCommon
//import org.apache.s2graph.core.rest.RequestParser
//
//import scala.concurrent.ExecutionContext
//
//trait IntegrateTinkerpopCommon extends IntegrateCommon {
//
//
//  lazy val g: S2Graph = new S2Graph(config, ExecutionContext.Implicits.global)
//
//  override def afterAll(): Unit = {
//    super.afterAll()
//    g.close()
//  }
//
//}
