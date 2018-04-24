package org.apache.s2graph.core.model

import org.apache.s2graph.core.utils.logger

import scala.concurrent.{ExecutionContext, Future}

trait ModelManager {
  private val ImportersLock = new java.util.concurrent.ConcurrentHashMap[String, Importer]

//  def importModel(labelName: String, importer: Importer)(implicit ec: ExecutionContext): Future[ImportStatus] = {
//    ImportersLock.computeIfAbsent(labelName, new java.util.function.Function[String, Importer] {
//      importer
//        .run()
//        .map { importer =>
//          logger.info(s"Close importer")
//          importer.close()
//
//          storagePool
//            .remove(storagePoolKey)
//            .foreach { oldStorage =>
//              logger.info(s"Delete old storage ($storagePoolKey) => $oldStorage")
//              oldStorage.flush()
//            }
//
//          val newStorage = importer.getImportedStorage(ec)
//          storagePool += storagePoolKey -> newStorage
//          logger.info(s"Insert new storage ($storagePoolKey) => $newStorage")
//          true
//        }
//        .onComplete { _ =>
//          logger.info(s"ImportLock release: $k")
//          ImportLock.remove(k)
//        }
//      importer
//    }
//  }

}
