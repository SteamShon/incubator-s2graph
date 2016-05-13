package org.apache.s2graph.core.tinkerpop.structure

import java.util

import org.apache.tinkerpop.gremlin.structure.util.ElementHelper
import org.apache.tinkerpop.gremlin.structure.{Property, Vertex, VertexProperty}
//
//class S2VertexProperty[V](val id: Any,
//                          val vertex: S2Vertex,
//                          val key: String,
//                          val value: V,
//                          val propertyKeyValues: Any*) extends VertexProperty[V](id, key) {
//
//  val properties: scala.collection.mutable.Map[String, Property[V]] = scala.collection.mutable.Map.empty
//
//  ElementHelper.legalPropertyKeyValueArray(propertyKeyValues)
//  ElementHelper.attachProperties(this, propertyKeyValues)
//
//  override def properties[U](strings: String*): util.Iterator[Property[U]] = {
//    val ls = new util.ArrayList[Property[U]]()
//    for {
//      (key, prop) <- properties
//    } {
//      ls.add(prop.asInstanceOf[Property[U]])
//    }
//    ls.iterator()
//  }
//
//  override def element(): Vertex = vertex
//
//  override def isPresent: Boolean = true
//
//
//  override def property[V](s: String, v: V): Property[V] = {
//    val prop = new S2Property[V](this, s, v)
//    properties.put(s, prop)
//    prop
//  }
//
//  override def remove(): Unit = ???
//
//}
