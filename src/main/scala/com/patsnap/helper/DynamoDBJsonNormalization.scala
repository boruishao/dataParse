//package com.patsnap.helper
//
//
//import io.circe.Json._
//
//
//trait DynamoDBJsonNormalization extends FieldParserHelper {
//
//  def updateField(json: Json): Json = transform(json)
//
//  def traverseThroughFields(json: Json, field: String): Json =
//    json.hcursor.downField(field).withFocus(updateField).top match {
//      case Some(result) => result
//      case None => json
//    }
//
//  def normalize(json: String): String = {
//    val doc = parse(json).getOrElse(empty)
//    val fields = doc.hcursor.fields.getOrElse(List())
//
//    fields.foldLeft(doc)(traverseThroughFields).toString
//  }
//}
//
//trait FieldParserHelper {
//
//  //sealed 修饰的 只能在当前文件被继承
//  sealed trait DynamoDBValue
//
//  case class DynamoDBString(s: String) extends DynamoDBValue
//
//  case class DynamoDBNumber(n: String) extends DynamoDBValue
//
//  //判断是否为整数
//  implicit class NumberChecker(number: Double) {
//    def isInteger = (number % 1) == 0
//  }
//
//  //偏函数
//  val isString: PartialFunction[Json, Result[DynamoDBValue]] = {
//    case json if json.as[DynamoDBString].isRight => json.as[DynamoDBString]
//  }
//
//  val isNumber: PartialFunction[Json, Result[DynamoDBValue]] = {
//    case json if json.as[DynamoDBNumber].isRight => json.as[DynamoDBNumber]
//  }
//
//  val isOther: PartialFunction[Json, Result[DynamoDBValue]] = {
//    case json => Right(DynamoDBString(json.asString.getOrElse("")))
//  }
//
//  val toStringJson: PartialFunction[Result[DynamoDBValue], Json] = {
//    case Right(DynamoDBString(string)) => Json.string(string)
//  }
//
//  val toDouble: PartialFunction[Result[DynamoDBValue], Double] = {
//    case Right(DynamoDBNumber(number)) => number.toDouble
//  }
//
//  val doubleToJson: PartialFunction[Double, Json] = {
//    case double => number(double).getOrElse(numberOrNull(0.0))
//  }
//
//  val toIntJson: PartialFunction[Double, Json] = {
//    case number if number.isInteger => int(number.toInt)
//  }
//
//  val defineCurrentType: PartialFunction[Json, Result[DynamoDBValue]] = isString orElse isNumber orElse isOther
//  val toNumberJson: PartialFunction[Result[DynamoDBValue], Json] = toDouble andThen (toIntJson orElse doubleToJson)
//  val castToPrimitive: PartialFunction[Result[DynamoDBValue], Json] = toStringJson orElse toNumberJson
//
//  val transform: PartialFunction[Json, Json] = defineCurrentType andThen castToPrimitive
//}
