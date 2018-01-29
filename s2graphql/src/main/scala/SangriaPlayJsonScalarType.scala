package org.apache.s2graph

import sangria.ast._
import sangria.schema._
import sangria.validation.ValueCoercionViolation

// https://gist.github.com/OlegIlyenko/5b96f4b54f656aac226d3c4bc33fd2a6

object PlayJsonPolyType {

  import sangria.ast
  import sangria.schema._
  import play.api.libs.json._

  case object JsonCoercionViolation extends ValueCoercionViolation("Not valid JSON")

  def scalarTypeToJsValue(v: sangria.ast.Value): JsValue = v match {
    case v: IntValue => JsNumber(v.value)
    case v: BigIntValue => JsNumber(BigDecimal(v.value.bigInteger))
    case v: FloatValue => JsNumber(v.value)
    case v: BigDecimalValue => JsNumber(v.value)
    case v: StringValue => JsString(v.value)
    case v: BooleanValue => JsBoolean(v.value)
    case v: ListValue => JsNull
    case v: VariableValue => JsNull
    case v: NullValue => JsNull
    case v: ObjectValue => JsNull
  }

  implicit val PolyType = ScalarType[JsValue]("Poly",
    description = Some("Type Poly = String | Number | Boolean"),
    coerceOutput = (value, _) ⇒ value match {
      case JsString(s) => s
      case JsNumber(n) => n
      case JsBoolean(b) => b
      case JsNull => null
      case _ => value
    },
    coerceUserInput = {
      case v: String => Right(JsString(v))
      case v: Boolean => Right(JsBoolean(v))
      case v: Int => Right(JsNumber(v))
      case v: Long => Right(JsNumber(v))
      case v: Float => Right(JsNumber(v.toDouble))
      case v: Double => Right(JsNumber(v))
      case v: BigInt => Right(JsNumber(BigDecimal(v)))
      case v: BigDecimal => Right(JsNumber(v))
      case _ => Left(JsonCoercionViolation)
    },
    coerceInput = {
      case value: ast.StringValue => Right(JsString(value.value))
      case value: ast.IntValue => Right(JsNumber(value.value))
      case value: ast.FloatValue => Right(JsNumber(value.value))
      case value: ast.BigIntValue => Right(JsNumber(BigDecimal(value.value.bigInteger)))
      case _ => Left(JsonCoercionViolation)
    })
}
