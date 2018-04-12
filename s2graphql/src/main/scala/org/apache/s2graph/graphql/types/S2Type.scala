/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.graphql.types

import scala.concurrent._
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core._
import org.apache.s2graph.core.schema._
import org.apache.s2graph.graphql.repository.GraphRepository
import sangria.schema._
import org.apache.s2graph.graphql.bind.{AstHelper, Unmarshaller}
import org.apache.s2graph.graphql.types.StaticTypes._

import scala.language.existentials

object S2Type {

  case class AddVertexParam(timestamp: Long,
                            id: Any,
                            columnName: String,
                            props: Map[String, Any])

  case class AddEdgeParam(ts: Long,
                          from: Any,
                          to: Any,
                          direction: String,
                          props: Map[String, Any])

  // Management params
  case class ServiceColumnParam(serviceName: String,
                                columnName: String,
                                props: Seq[Prop] = Nil)

  def makeField[A](name: String, cType: String, tpe: ScalarType[A]): Field[GraphRepository, Any] =
    Field(name,
      OptionType(tpe),
      description = Option("desc here"),
      resolve = c => c.value match {
        case v: S2VertexLike => name match {
          case "timestamp" => v.ts.asInstanceOf[A]
          case _ =>
            val innerVal = v.propertyValue(name).get
            JSONParser.innerValToAny(innerVal, cType).asInstanceOf[A]
        }
        case e: S2EdgeLike => name match {
          case "timestamp" => e.ts.asInstanceOf[A]
          case "direction" => e.getDirection().asInstanceOf[A]
          case _ =>
            val innerVal = e.propertyValue(name).get.innerVal
            JSONParser.innerValToAny(innerVal, cType).asInstanceOf[A]
        }
        case _ =>
          throw new RuntimeException(s"Error on resolving field: ${name}, ${cType}, ${c.value.getClass}")
      }
    )

  def makePropField(cName: String, cType: String): Field[GraphRepository, Any] = cType match {
    case "boolean" | "bool" => makeField[Boolean](cName, cType, BooleanType)
    case "string" | "str" | "s" => makeField[String](cName, cType, StringType)
    case "int" | "integer" | "i" | "int32" | "integer32" => makeField[Int](cName, cType, IntType)
    case "long" | "l" | "int64" | "integer64" => makeField[Long](cName, cType, LongType)
    case "double" | "d" => makeField[Double](cName, cType, FloatType)
    case "float64" | "float" | "f" | "float32" => makeField[Double](cName, "double", FloatType)
    case _ => throw new RuntimeException(s"Cannot support data type: ${cType}")
  }

  def makeInputFieldsOnService(service: Service): Seq[InputField[Any]] = {
    val inputFields = service.serviceColumns(false).map { serviceColumn =>
      val idField = InputField("id", toScalarType(serviceColumn.columnType))
      val propFields = serviceColumn.metasWithoutCache.filter(ColumnMeta.isValid).map { lm =>
        InputField(lm.name, OptionInputType(toScalarType(lm.dataType)))
      }

      val vertexMutateType = InputObjectType[Map[String, Any]](
        s"Input_${service.serviceName}_${serviceColumn.columnName}_vertex_mutate",
        description = "desc here",
        () => idField :: propFields
      )

      InputField[Any](serviceColumn.columnName, OptionInputType(ListInputType(vertexMutateType)))
    }

    inputFields
  }

  def makeInputFieldsOnLabel(label: Label): Seq[InputField[Any]] = {
    val propFields = label.labelMetaSet.toList.map { lm =>
      InputField(lm.name, OptionInputType(toScalarType(lm.dataType)))
    }

    val labelFields = List(
      InputField("timestamp", OptionInputType(LongType)),
      InputField("from", toScalarType(label.srcColumnType)),
      InputField("to", toScalarType(label.srcColumnType)),
      InputField("direction", OptionInputType(BothDirectionType))
    )

    labelFields.asInstanceOf[Seq[InputField[Any]]] ++ propFields.asInstanceOf[Seq[InputField[Any]]]
  }

  def makeServiceColumnFields(column: ServiceColumn, allLabels: Seq[Label]): List[Field[GraphRepository, Any]] = {
    val reservedFields = List("id" -> column.columnType, "timestamp" -> "long")
    val columnMetasKv = column.metasWithoutCache.filter(ColumnMeta.isValid).map { columnMeta => columnMeta.name -> columnMeta.dataType }

    val (sameLabel, diffLabel) = allLabels.toList.partition(l => l.srcColumn == l.tgtColumn)

    val outLabels = diffLabel.filter(l => column == l.srcColumn).distinct.toList
    val inLabels = diffLabel.filter(l => column == l.tgtColumn).distinct.toList
    val inOutLabels = sameLabel.filter(l => l.srcColumn == column && l.tgtColumn == column)

    lazy val columnFields = (reservedFields ++ columnMetasKv).map { case (k, v) => makePropField(k, v) }

    lazy val outLabelFields: List[Field[GraphRepository, Any]] = outLabels.map(l => makeLabelField("out", l, allLabels))
    lazy val inLabelFields: List[Field[GraphRepository, Any]] = inLabels.map(l => makeLabelField("in", l, allLabels))
    lazy val inOutLabelFields: List[Field[GraphRepository, Any]] = inOutLabels.map(l => makeLabelField("both", l, allLabels))
    lazy val propsType = wrapField(s"ServiceColumn_${column.service.serviceName}_${column.columnName}_props", "props", columnFields)

    lazy val labelFieldNameSet = (outLabels ++ inLabels ++ inOutLabels).map(_.label).toSet

    propsType :: inLabelFields ++ outLabelFields ++ inOutLabelFields ++ columnFields.filterNot(cf => labelFieldNameSet(cf.name))
  }

  def makeServiceField(service: Service, allLabels: List[Label])(implicit repo: GraphRepository): List[Field[GraphRepository, Any]] = {
    lazy val columnsOnService = service.serviceColumns(false).toList.map { column =>
      lazy val serviceColumnFields = makeServiceColumnFields(column, allLabels)
      lazy val ColumnType = ObjectType(
        s"ServiceColumn_${service.serviceName}_${column.columnName}",
        () => fields[GraphRepository, Any](serviceColumnFields: _*)
      )

      val v = Field(column.columnName,
        ListType(ColumnType),
        arguments = List(
          Argument("id", OptionInputType(toScalarType(column.columnType))),
          Argument("ids", OptionInputType(ListInputType(toScalarType(column.columnType))))
        ),
        description = Option("desc here"),
        resolve = c => {
          implicit val ec = c.ctx.ec
          val (vertices, canSkipFetchVertex) = Unmarshaller.serviceColumnFieldOnService(column, c)

          if (canSkipFetchVertex) Future.successful(vertices)
          else c.ctx.getVertices(vertices)
        }
      ): Field[GraphRepository, Any]

      v
    }

    columnsOnService
  }

  def makeLabelField(dir: String, label: Label, allLabels: Seq[Label]): Field[GraphRepository, Any] = {
    val labelReserved = List("direction" -> "string", "timestamp" -> "long")
    val labelProps = label.labelMetas.map { lm => lm.name -> lm.dataType }

    val column = if (dir == "out") label.tgtColumn else label.srcColumn

    lazy val labelFields: List[Field[GraphRepository, Any]] =
      (labelReserved ++ labelProps).map { case (k, v) => makePropField(k, v) }

    lazy val labelPropField = wrapField(s"Label_${label.label}_props", "props", labelFields)

    lazy val labelColumnType = ObjectType(s"Label_${label.label}_${column.columnName}",
      () => makeServiceColumnFields(column, allLabels)
    )

    lazy val serviceColumnField: Field[GraphRepository, Any] = Field(column.columnName, labelColumnType, resolve = c => {
      implicit val ec = c.ctx.ec
      val (vertex, canSkipFetchVertex) = Unmarshaller.serviceColumnFieldOnLabel(c)

      if (canSkipFetchVertex) Future.successful(vertex)
      else c.ctx.getVertices(Seq(vertex)).map(_.head) // fill props
    })

    lazy val EdgeType = ObjectType(
      s"Label_${label.label}_${column.columnName}_${dir}",
      () => fields[GraphRepository, Any](
        List(serviceColumnField, labelPropField) ++ labelFields.filterNot(_.name == column.columnName): _*)
    )

    val dirArgs = dir match {
      case "in" => Argument("direction", OptionInputType(InDirectionType), "desc here", defaultValue = "in") :: Nil
      case "out" => Argument("direction", OptionInputType(OutDirectionType), "desc here", defaultValue = "out") :: Nil
      case "both" => Argument("direction", OptionInputType(BothDirectionType), "desc here", defaultValue = "out") :: Nil
    }

    lazy val edgeTypeField: Field[GraphRepository, Any] = Field(
      s"${label.label}",
      ListType(EdgeType),
      arguments = dirArgs,
      description = Some("fetch edges"),
      resolve = { c =>
        val (vertex, dir) = Unmarshaller.labelField(c)
        c.ctx.getEdges(vertex, label, dir)
      }
    )

    edgeTypeField
  }

}

class S2Type(repo: GraphRepository) {

  import S2Type._
  import org.apache.s2graph.graphql.bind.Unmarshaller._

  implicit val graphRepository = repo

  /**
    * fields
    */
  lazy val serviceFields: List[Field[GraphRepository, Any]] = repo.allServices.map { service =>
    lazy val serviceFields = DummyObjectTypeField :: makeServiceField(service, repo.allLabels())

    lazy val ServiceType = ObjectType(
      s"Service_${service.serviceName}",
      fields[GraphRepository, Any](serviceFields: _*)
    )

    Field(
      service.serviceName,
      ServiceType,
      description = Some(s"serviceName: ${service.serviceName}"),
      resolve = _ => service
    ): Field[GraphRepository, Any]
  }

  /**
    * arguments
    */
  lazy val addVertexArg = {
    val serviceArguments = repo.allServices().map { service =>
      val serviceFields = DummyInputField +: makeInputFieldsOnService(service)

      val ServiceInputType = InputObjectType[List[AddVertexParam]](
        s"Input_vertex_${service.serviceName}_param",
        () => serviceFields.toList
      )
      Argument(service.serviceName, OptionInputType(ServiceInputType))
    }

    serviceArguments
  }

  lazy val addEdgeArg = {
    val labelArguments = repo.allLabels().map { label =>
      val labelFields = DummyInputField +: makeInputFieldsOnLabel(label)
      val labelInputType = InputObjectType[AddEdgeParam](
        s"Input_label_${label.label}_param",
        () => labelFields.toList
      )

      Argument(label.label, OptionInputType(ListInputType(labelInputType)))
    }

    labelArguments
  }

  /**
    * Query fields
    * Provide s2graph query / mutate API
    * - Fields is created(or changed) for metadata is changed.
    */
  lazy val queryFields = serviceFields

  lazy val mutationFields: List[Field[GraphRepository, Any]] = List(
    Field("addVertex",
      ListType(MutateResponseType),
      arguments = addVertexArg,
      resolve = c => {
        val vertices = c.args.raw.keys.flatMap { serviceName =>
          val addVertexParams = c.arg[List[AddVertexParam]](serviceName)
          addVertexParams.map { param =>
            repo.toS2VertexLike(serviceName, param)
          }
        }

        c.ctx.addVertices(vertices.toSeq)
      }
    ),
    Field("addEdge",
      ListType(MutateResponseType),
      arguments = addEdgeArg,
      resolve = c => {
        val edges = c.args.raw.keys.flatMap { labelName =>
          val addEdgeParams = c.arg[Vector[AddEdgeParam]](labelName)
          addEdgeParams.map { param =>
            repo.toS2EdgeLike(labelName, param)
          }
        }

        c.ctx.addEdges(edges.toSeq)
      }
    )
  )
}
