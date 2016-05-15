//package org.apache.s2graph.core.tinkerpop.structure
//
//import org.apache.tinkerpop.gremlin.structure.util.ElementHelper
//import org.apache.tinkerpop.gremlin.structure.{Property, Vertex, VertexProperty}
//import scala.collection.JavaConversions._
//import java.util
//
//class S2VertexProperty[V](val id: AnyRef,
//                          val vertex: S2Vertex,
//                          val key: String,
//                          val value: V,
//                          val kvs: AnyRef*) extends VertexProperty[V] {
//
//  val properties: scala.collection.mutable.Map[String, Property[_]] = scala.collection.mutable.Map.empty
//
//  ElementHelper.legalPropertyKeyValueArray(kvs)
//  ElementHelper.attachProperties(vertex, kvs)
//
//  override def properties[U](keys: String*): util.Iterator[Property[U]] = {
//    val set = for {
//      key <- keys
//      prop = property[U](key) if prop != null
//    } yield {
//      prop
//    }
//    set.iterator
//  }
//
//  override def element(): Vertex = vertex
//
//  override def isPresent: Boolean = true
//
//  override def property[U](key: String, v: U): Property[U] = {
//    val newProperty = new S2Property[U](vertex, key, v)
//    properties.put(key, newProperty)
//    newProperty
//  }
//
//  override def remove(): Unit = ???
//
//}
