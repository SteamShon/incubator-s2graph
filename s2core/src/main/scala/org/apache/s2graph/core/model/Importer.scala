package org.apache.s2graph.core.model

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import scala.concurrent.{ExecutionContext, Future}

trait Importer {
  def run()(implicit ec: ExecutionContext): Future[Importer]

  def status: ImportStatus

//  def getImportedStorage(graphExecutionContext: ExecutionContext): Storage[_, _]
  def close(): Unit
}

case class HDFSImporter(hadoopConfig: Configuration) extends Importer {
  override def run()(implicit ec: ExecutionContext): Future[Importer] = {
    // file copy
    Future.successful(this)
  }

  override def status: ImportStatus = ???

  override def close(): Unit = ???
}