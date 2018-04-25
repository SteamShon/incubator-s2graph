package org.apache.s2graph.core.model

import com.typesafe.config.Config
import org.apache.s2graph.core.schema.Label
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.{Fetcher, S2GraphLike}

import scala.concurrent.{ExecutionContext, Future}

object ModelManager {
  val FetcherClassNameKey = "fetchClassName"
  val ImporterClassNameKey = "importerClassName"
}

class ModelManager(s2GraphLike: S2GraphLike) {
  import ModelManager._

  private val fetcherPool = scala.collection.mutable.Map.empty[String, Fetcher]
  private val ImportLock = new java.util.concurrent.ConcurrentHashMap[String, Importer]

  def toImportLockKey(label: Label): String = label.label

  def getFetcher(label: Label): Fetcher = {
    fetcherPool.getOrElse(toImportLockKey(label), throw new IllegalStateException(s"$label is not imported."))
  }

  def initImporter(config: Config): Importer = {
    val className = config.getString(ImporterClassNameKey)
    className match {
      case "hdfs" => HDFSImporter(s2GraphLike, config)
      case _ => IdentityImporter()
    }
  }

  def initFetcher(config: Config)(implicit ec: ExecutionContext): Future[Fetcher] = {
    val className = config.getString(FetcherClassNameKey)
    //TODO: ClassForName.
    className match {
      case "memory" =>
        val fetcher = new MemoryModelFetcher(s2GraphLike)
        fetcher.init(config)
      case "annoy" =>
        val fetcher = new AnnoyModelFetcher(s2GraphLike)
        fetcher.init(config)
      case _ => throw new IllegalArgumentException(s"not implemented yet.")
    }
  }

  def importModel(label: Label, config: Config)(implicit ec: ExecutionContext): Future[Importer] = {
    val importer = ImportLock.computeIfAbsent(toImportLockKey(label), new java.util.function.Function[String, Importer] {
      override def apply(k: String): Importer = {
        val importer = initImporter(config.getConfig("importer"))

        //TODO: Update Label's extra options.
        importer
          .run()
          .map { importer =>
            logger.info(s"Close importer")
            importer.close()

            initFetcher(config.getConfig("fetcher")).map { fetcher =>
              importer.setStatus(true)


              fetcherPool
                .remove(k)
                .foreach { oldFetcher =>
                  logger.info(s"Delete old storage ($k) => $oldFetcher")
                  oldFetcher.close()
                }

              fetcherPool += (k -> fetcher)
              true
            }

            true
          }
          .onComplete { _ =>
            logger.info(s"ImportLock release: $k")
            ImportLock.remove(k)
          }
        importer
      }
    })

    Future.successful(importer)
  }

}
