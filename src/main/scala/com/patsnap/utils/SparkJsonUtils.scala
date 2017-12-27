package com.patsnap.utils

import java.math.BigInteger
import java.security.MessageDigest
import java.util
import java.util.UUID
import java.util.regex.{Matcher, Pattern}

import net.sf.json.{JSONArray, JSONObject}

object SparkJsonUtils {

  var md : MessageDigest = MessageDigest.getInstance("MD5")

  /**
    * 通过正则表达式解析json
    *
    * @param str
    * @return
    */
  def horizontalParseJson(str:String):String= str.substring(32, str.length - 4)
    .replaceAll("\\{\"s\":|\\{\"n\":|\\{\"m\":|\\{\"l\":", "")
    .replaceAll("]}", "]")
    .replaceAll("\"}", "\"")
    .replaceAll("}}", "}") + "}"

  var rowid =""

  /**
    * 上述的递归方法实现
    * 数据类型分为4种：m l s n
    * 有效的键刨除上面4个，暂定是长度大于1的，切两边要有双引号
    * 每条数据需要有一个rowid
    * 针对 n （数字）：只需把键值求出，并拼接
    * 针对 s (字符串)：需要把值两边加上引号
    * 针对 m （对象）: 每个对象需要添加uuid（后续要将对象单独提出一张表，做主键），递归对象
    * 针对 l （集合）：遍历集合递归对象
    *
    * @param json
    * @param sb
    */
  private def parseJson(json: JSONObject, sb: StringBuffer): Unit = {
    val keySet = json.keySet
    import scala.collection.JavaConversions._
    for (key <- keySet) {
      val k = key.asInstanceOf[String]
      val s = json.getString(k)
      if ("l" == k) {
        sb.append("[")
        val fromObject = JSONArray.fromObject(s)
        for (obj <- fromObject) {
          val json2 = JSONObject.fromObject(obj)
          parseJson(json2, sb)
          sb.setLength(sb.length - 1)
          sb.append(",")
        }
        //如果list是空的，就会以[开头，则不需要把逗号去掉
        if (sb.lastIndexOf("[") != sb.length - 1) sb.setLength(sb.length - 1)
        sb.append("]").append(",")
      }
      if ("m" == k) {
        sb.append("{")
        val json2 = JSONObject.fromObject(s)
        val uuid = "{\"s\":\"" + getUUid + "\"}" //如果是map需要生成符合格式的uuid
        json2.put("uuid", uuid)
        json2.put("rowid", rowid)
        parseJson(json2, sb)
        sb.setLength(sb.length - 1)
        sb.append("}").append(",")
      }
      if ("n" == k) sb.append(s).append(",")
      if ("s" == k) sb.append("\"").append(s).append("\"").append(",")
      //拼接key
      if (k.length > 1) {
        if ("country" == k) rowid = "{\"s\"" + s.substring(4, s.length - 1) + "}"
        sb.append("\"").append(key).append("\"").append(":")
        val json2 = json.getJSONObject(k)
        parseJson(json2, sb)
      }
    }
  }

  private def getUUid = {
    val uuid = UUID.randomUUID
    val guidStr = uuid.toString
    md.update(guidStr.getBytes, 0, guidStr.length)
    new BigInteger(1, md.digest).toString(16)
  }


  def longitudinalParseJson(str:String):String={
    val jsonObject = JSONObject.fromObject(str.substring(32, str.length - 3))
    val sb = new StringBuffer
    sb.append("{")
    parseJson(jsonObject, sb)
    if (sb.length != 0) if (sb.lastIndexOf("]") != sb.length - 1) sb.setLength(sb.length - 1)
    sb.append('}')
    sb.toString
  }

  /**
    * 取出list中的部分
    *
    * @param listName
    * @param json
    * @return
    */
  def getList(listName: String, json: String): String = {
    val regex: String = "\"" + listName + "\":\\[.*?]"
    //"\"[^,]*?\\[.*?]" 这个是匹配通用的
    val pattern: Pattern = Pattern.compile(regex)
    val matcher: Matcher = pattern.matcher(json)
    if (matcher.find) {
      var group: String = matcher.group(0)
      group = group.substring(0, group.length - 1)
      return  group.replaceAll("\".*?\":\\[", "")
    }
    ""
  }

//  def getList(json:String):String ={
//    val nObject = JSONObject.fromObject(json)
//
//    val regex: String ="\"[^,]*?\\[.*?]"
//    val pattern: Pattern = Pattern.compile(regex)
//    val matcher: Matcher = pattern.matcher(json)
//    while (matcher.find()){
//      var group :String = matcher.group(0)
//
//    }
//
//  }

  /**
    * 遍历出list中的map
    *
    * @param string
    * @return
    */
  def getMapInList(string: String): Array[String] = {
    val list: util.List[String] = new util.ArrayList[String]
    val regex: String = "\\{.*?}"
    val pattern: Pattern = Pattern.compile(regex)
    val matcher: Matcher = pattern.matcher(string)
    while ( {
      matcher.find
    }) list.add(matcher.group(0))
    val strings: Array[String] = list.toArray(new Array[String](list.size))
    strings
  }

  /**
    * 获取不在list中的map
    *
    * @param mapName
    * @param json
    * @return
    */
  def getMap(mapName: String, json: String): String = {
    var str: String = ""
    val regex: String = "\"" + mapName + "\":\\{.*?}"
    val pattern: Pattern = Pattern.compile(regex)
    val matcher: Matcher = pattern.matcher(json)
    if (matcher.find) {
      val group: String = matcher.group(0)
      str = group.replaceAll("\".*?\":\\{", "{")
    }
    str
  }

  /**
    * 将json中的list对象转为uuid的数组
    *
    * @param json
    * @return
    */
  def list2uuid(json: String): String = {
    val regex: String = "\"[^,{]*?\\[.*?]"
    val jsonobject: JSONObject = JSONObject.fromObject(json)
    val pattern: Pattern = Pattern.compile(regex)
    val matcher: Matcher = pattern.matcher(json)
    while ( {
      matcher.find
    }) {
      var group: String = matcher.group(0)
      group = group.substring(0, group.length)
      val s: String = group.substring(group.indexOf(":") + 1)
      val key: String = group.substring(1, group.indexOf("\"", 2))
      val jsonArray: JSONArray = JSONArray.fromObject(s)
      val newArray: JSONArray = new JSONArray
      import scala.collection.JavaConversions._
      for (jsonobj <- jsonArray) {
        val jsonObject: JSONObject = JSONObject.fromObject(jsonobj)
        val uuid: Any = jsonObject.get("uuid")
        newArray.add(uuid)
      }
      jsonobject.put(key, newArray)
    }
    jsonobject.toString
  }

  /**
    * 把指定的map，替换成uuid
    *
    * @param mapName
    * @param json
    * @return
    */
  def map2uuid(mapName: String, json: String): String = {
    val jsonObject: JSONObject = JSONObject.fromObject(json)
    val trademark_source_id: Any = jsonObject.get(mapName)
    val jsonObject1: JSONObject = JSONObject.fromObject(trademark_source_id)
    jsonObject.put(mapName, jsonObject1.get("uuid"))
    jsonObject.toString
  }

  /**
    * 通用匹配map，但是嵌套的map不行
    *
    * @param json
    * @return
    */
  def map2uuid(json: String): String = {
    val regex: String = "\"[^,\\[]*?\\{.*?}"
    val jsonObject: JSONObject = JSONObject.fromObject(json)
    val pattern: Pattern = Pattern.compile(regex)
    val matcher: Matcher = pattern.matcher(json)
    while ( {
      matcher.find
    }) {
      val group: String = matcher.group(0)
      val key: String = group.substring(1, group.indexOf("\"", 2))
      val replace: String = group.replaceAll("\".*?\":\\{", "{")
      val jsonObject1: JSONObject = JSONObject.fromObject(replace)
      jsonObject.put(key, jsonObject1.get("uuid"))
    }
    jsonObject.toString
  }

}
