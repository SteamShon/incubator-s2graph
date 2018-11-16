package org.apache.s2graph.core.storage.dynamo

import com.typesafe.config.Config
import org.apache.s2graph.core.storage.StorageManagement
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest

import scala.compat.java8.FutureConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class DynamoDBStorageManagement(client: DynamoDbAsyncClient) extends StorageManagement {
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
  override def createTable(config: Config, tableNameStr: String): Unit = {
    val request = CreateTableRequest.builder()
      .tableName(tableNameStr)
      .build()

    val future = toScala(client.createTable(request))
    Await.result(future, Duration("60 seconds"))
  }

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
