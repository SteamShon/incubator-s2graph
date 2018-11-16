package org.apache.s2graph.core.storage.dynamo

import com.typesafe.config.Config
import org.apache.s2graph.core.storage.StorageManagement

class DynamoDBStorageManagement extends StorageManagement {
  /**
    * this method need to be called when client shutdown. this is responsible to cleanUp the resources
    * such as client into storage.
    */
  override def flush(): Unit = ???

  /**
    * create table on storage.
    * if storage implementation does not support namespace or table, then there is nothing to be done
    *
    * @param config
    */
  override def createTable(config: Config, tableNameStr: String): Unit = ???

  /**
    *
    * @param config
    * @param tableNameStr
    */
  override def truncateTable(config: Config, tableNameStr: String): Unit = ???

  /**
    *
    * @param config
    * @param tableNameStr
    */
  override def deleteTable(config: Config, tableNameStr: String): Unit = ???

  /**
    *
    */
  override def shutdown(): Unit = ???
}
