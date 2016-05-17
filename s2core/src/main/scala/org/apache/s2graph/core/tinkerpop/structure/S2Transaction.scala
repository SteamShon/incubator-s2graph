package org.apache.s2graph.core.tinkerpop.structure

import java.util.function.{Function, Consumer}

import org.apache.tinkerpop.gremlin.structure.Transaction.{Workload, Status}
import org.apache.tinkerpop.gremlin.structure.{Graph, Transaction}

class S2Transaction extends Transaction {
  override def onReadWrite(consumer: Consumer[Transaction]): Transaction = ???

  override def rollback(): Unit = ???

  override def onClose(consumer: Consumer[Transaction]): Transaction = ???

  override def addTransactionListener(consumer: Consumer[Status]): Unit = ???

  override def readWrite(): Unit = ???

  override def isOpen: Boolean = ???

  override def close(): Unit = ???

  override def clearTransactionListeners(): Unit = ???

  override def open(): Unit = ???

  override def submit[R](function: Function[Graph, R]): Workload[R] = ???

  override def removeTransactionListener(consumer: Consumer[Status]): Unit = ???

  override def createThreadedTx[G <: Graph](): G = ???

  override def commit(): Unit = ???
}
