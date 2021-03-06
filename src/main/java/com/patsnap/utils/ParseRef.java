package com.patsnap.utils;

import com.amazonaws.services.dynamodbv2.document.Item;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ParseRef {

    private static final String ID_NAME = "\"row_id\":";
    private static final String PID_NAME = "\"pid\":";
    private static final String ARRAY_LIST = "ArrayList";
    private static final String LINKED_HASH_MAP="LinkedHashMap";
    private static final String BIG_DECIMAL= "BigDecimal";
    private static MessageDigest md;

    static {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    /**
     * 以字符串的形式，解析json，把引用类型变为单独的json，key是表名，value是json
     * @param dyJson
     * @return
     */
    public static Map<String,String> getNormalJson(String dyJson,String tableName,String idName){
        HashMap<String,String> result = new HashMap<>();

        //初始化json和map
        JSONObject jsonObject = JSONObject.fromObject(dyJson.substring(32, dyJson.length() - 3));
        Map<String, StringBuffer> map = new LinkedHashMap<>();
        map.put(tableName,new StringBuffer().append("{"));

        //获取id 这里id可以动态获取，或者自己生成
        String pid ="\""+JSONObject.fromObject(jsonObject.get(idName)).get("s").toString()+"\"";
        parse(jsonObject, map,tableName,pid);
        if (map.get(tableName).lastIndexOf("]") != map.get(tableName).length()- 1) {
            map.get(tableName).setLength(map.get(tableName).length()-1);
        }
        map.get(tableName).append("}");

        for ( Map.Entry<String,StringBuffer> entry: map.entrySet()) {
            result.put(entry.getKey(),entry.getValue().toString());
        }
        return result;
    }

    private static void parse(JSONObject jsonObj, Map<String,StringBuffer> tableMap,String tableName,String id){
        parse(jsonObj, tableMap, tableName, id,false);
    }

    /**
     * 递归方法
     * map 单独存一个json
     * list在一个json中，并列存放
     * @param jsonObj
     * @param tableMap
     * @param tableName
     * @param pid
     * @param isList
     * bug: 在list的map元素中 如果又出现子map，那么存放的id如果还是最外层id的话就会出现多对多的情况，所以存在id只能是上层对象的id
     *      那么在程序中，id名一定要具规律才能取到，或者添加id字段（即，要有一个parentId找到上级，有一个id作为唯一标识）
     */
    private static void parse(JSONObject jsonObj, Map<String,StringBuffer> tableMap,String tableName,String pid,boolean isList){
        Set keys = jsonObj.keySet();
        //获取子表的表名，通过取父json最后拼接的词
        StringBuffer parentSb = tableMap.get(tableName);
//        String idName = pid.substring(0,pid.indexOf(":")+1);
//        String idValue = pid.substring(pid.indexOf(":")+1,pid.length());
        //生成id
        String id = "\"" +getUUid() + "\"" ;

        String configTabName="";
        if(parentSb!=null&&parentSb.toString().indexOf("\"")>-1){
            String s = parentSb.toString();
            configTabName = tableName+"_"+s.substring(s.lastIndexOf("\"", s.lastIndexOf("\"") - 1) + 1, s.lastIndexOf("\""));
        }
        for (Object o : keys) {
            String k = (String)o;
            String v = jsonObj.getString(k);
            if("l".equals(k)){
                //字表初始化（获取名字，生成json）
//                tableMap.get(tableName).append("{\"configTabName\":").append("\""+configTabName+"\"}").append(",");
                //把list类型的字段名从json中删除
                tableMap.get(tableName).delete(tableMap.get(tableName).lastIndexOf(","),tableMap.get(tableName).length()).append(",");

                if(tableMap.keySet().contains(configTabName)){
                    throw new RuntimeException("与主表名相同，或存在相同的子表名:"+ configTabName);
                }
                tableMap.put(configTabName,new StringBuffer().append("["));
                JSONArray jsonArray = JSONArray.fromObject(v);
                for (Object json2 : jsonArray) {
                    parse(JSONObject.fromObject(json2),tableMap,configTabName,pid,true);
                    tableMap.get(configTabName).append(",");
                }
                if (tableMap.get(configTabName).lastIndexOf("[") != tableMap.get(configTabName).length()- 1) {
                    tableMap.get(configTabName).setLength(tableMap.get(configTabName).length()-1);
                }
                tableMap.get(configTabName).append("]");
            }
            if("m".equals(k)){
                //这里分两种，list中的map和外置map，list中的map要求把所有的写在一条json里，外置的要每个单独写一个json
                if(isList){
                    tableMap.get(tableName).append("{")
                            .append(ID_NAME).append(id).append(",")
                            .append(PID_NAME).append(pid).append(",");
                    parse(JSONObject.fromObject(v),tableMap,tableName,id,false);
                    tableMap.get(tableName).setLength(tableMap.get(tableName).length() - 1);
                    tableMap.get(tableName).append("}");
                }else {
                    //非list的map要新建一个json
//                    tableMap.get(tableName).append("{\"configTabName\":").append("\"" + configTabName + "\"}").append(",");
                    //把map或者list类型的字段名从json中删除
                    tableMap.get(tableName).delete(tableMap.get(tableName).lastIndexOf(","),tableMap.get(tableName).length()).append(",");
                    if(tableMap.keySet().contains(configTabName)){
                        throw new RuntimeException("与主表名相同，或存在相同的子表名:"+ configTabName);
                    }
                    tableMap.put(configTabName, new StringBuffer());
                    tableMap.get(configTabName).append("{")
                            .append(ID_NAME).append(id).append(",")
                            .append(PID_NAME).append(pid).append(",");
                    parse(JSONObject.fromObject(v), tableMap, configTabName, id,false);
                    tableMap.get(configTabName).setLength(tableMap.get(configTabName).length() - 1);
                    tableMap.get(configTabName).append("}");
                }
            }
            if("n".equals(k)){
                tableMap.get(tableName).append(v).append(",");
            }
            if("s".equals(k)){
                tableMap.get(tableName).append("\"").append(v).append("\"").append(",");
            }
            if(k.length()>1){
                tableMap.get(tableName).append("\"").append(k).append("\"").append(":");
                JSONObject json2 = jsonObj.getJSONObject(k);
                parse(json2,tableMap,tableName,pid,false);
            }
        }
    }

    /**********************************************************************************************************************************/

    /**
     * 使用dynamo的item类解析json
     * @param dyJson
     * @param tableName
     * @param idName
     * @return
     */
    public static Map<String,String> getNormalJsonByItem(String dyJson,String tableName,String idName){
        HashMap<String,String> result = new LinkedHashMap<>();
        Item it = new Item().fromJSON(dyJson.substring(32, dyJson.length() - 3));//init item
        Map<String,Item> itemMap = new LinkedHashMap<>();
        itemMap.put(tableName,new Item());
        Map<String,String> idMap = new LinkedHashMap<>();
        String id = it.getMap(idName).get("s").toString();
        idMap.put(idName,id);
//        parseItem(tableName,it,itemMap,null,idName,id);
        parseItem(tableName,it,itemMap,null,idMap);
        for (String key : itemMap.keySet()) {
            result.put(key,itemMap.get(key).toJSON());
        }
        return result;
    }

    public static void parseItem(String fileName, Item item , Map<String,Item> itemMap, String pFieldName, Map<String,String> idMap){
        try {
            parseItem(fileName,item,itemMap,pFieldName,idMap,false);
        } catch (Exception e) {
            Map.Entry<String, String> pidEntry=null;
            if(idMap.size()>0) {
                pidEntry = idMap.entrySet().iterator().next();
                System.out.println("错误的id是:"+pidEntry.getValue());
            }
            e.printStackTrace();
        }
    }
    /**
     *
     * @param fileName
     * @param item
     * @param itemMap
     * @param pFieldName
     * @param idMap 子表需添加的id
     * @param isList
     */
    public static void parseItem(String fileName, Item item , Map<String,Item> itemMap, String pFieldName, Map<String,String> idMap,boolean isList){
        Map<String, Object> eles = item.asMap();
        String fName = fileName+"_"+pFieldName; //新建文件名
        //主id的名字和值
        Map.Entry<String, String> pidEntry=null;
        if(idMap.size()>0) {
            pidEntry = idMap.entrySet().iterator().next();
        }

        for (String k : eles.keySet()){
            if(item.getTypeOf(k).getSimpleName().equals("ArrayList")){
                ArrayList<Map<String, Object>> maps = (ArrayList<Map<String, Object>>) item.get(k);
                Item listItem = new Item();
                int i = 0;
                for ( Map<String, Object> m: maps) {
                    String element = "list_target"+i;
                    itemMap.put(fName,new Item());
                    Item it = new Item().fromMap(m);
                    idMap.put(pFieldName+"_row",i+"");
                    parseItem(fName,it,itemMap,null,idMap,true);//作为从list递归的标志
                    listItem.withMap(element, itemMap.get(fName).asMap());//以此拼接item到list中
                    itemMap.remove(fName);//删除临时item
                    i++;
                }
                idMap.clear();
                if(pidEntry!=null){
                    idMap.put(pidEntry.getKey(),pidEntry.getValue());
                }
                if(listItem.numberOfAttributes()>0) {
                    itemMap.put(fName, listItem);//把list类型的数据添加
                }
            }
            if(item.getTypeOf(k).getSimpleName().equals("LinkedHashMap")){
                Item it = new Item().fromMap((Map<String, Object>) item.get(k));
                if("m".equals(k)){
                    //给新建对象添加主键
                    for (Map.Entry<String,String> identry : idMap.entrySet()) {
                        it.withJSON(identry.getKey(),new Item().withString("s",identry.getValue()).toJSON());
                    }
                    if(isList){ //从list传来的map，要写在同一个文件里，作此处理
                        parseItem(fileName,it,itemMap,null,idMap,false); //map类型不需要字段名
                    }else {
                        itemMap.put(fName, new Item());
                        parseItem(fName, it, itemMap, null,idMap,false); //map类型不需要字段名
                    }
                }else {
                    parseItem(fileName,it,itemMap,k,idMap,false);
                }
            }
            if(item.getTypeOf(k).getSimpleName().equals("BigDecimal")){
                if(isList){//从list中传来的string或number，没有字段命，存为以下名称
                    itemMap.get(fileName).withString("number_field",item.getString(k));
                    //添加主键
                    for (Map.Entry<String,String> identry : idMap.entrySet()) {
                        itemMap.get(fileName).withString(identry.getKey(),identry.getValue());
                    }
                }else {
                    itemMap.get(fileName).withNumber(pFieldName, item.getNumber(k));
                }
            }
            if(item.getTypeOf(k).getSimpleName().equals("String")){
                if(isList){
                    itemMap.get(fileName).withString("string_field",item.getString(k));
                    //添加主键
                    for (Map.Entry<String,String> identry : idMap.entrySet()) {
                        itemMap.get(fileName).withString(identry.getKey(),identry.getValue());
                    }
                }else {
                    itemMap.get(fileName).withString(pFieldName,item.getString(k));
                }
            }
        }
    }

    /**********************************************************************************************************************************/

    /**
     * 使用dynamo的item类解析json
     * @param dyJson
     * @param tableName
     * @param idName
     * @return
     */
    public static Map<String,String> parseNormalJsonByItem(String dyJson,String tableName,String idName){

        HashMap<String,String> result = new LinkedHashMap<>();
        try {
        Item it = new Item().fromJSON(dyJson);//init item
        Map<String,Item> itemMap = new LinkedHashMap<>();
        itemMap.put(tableName,new Item());
        Map<String,String> idMap = new LinkedHashMap<>();
        String id = it.getString(idName).toString();
        idMap.put(idName,id);

            parseNormalItem(tableName,it,itemMap,idMap);

        for (String key : itemMap.keySet()) {
            result.put(key,itemMap.get(key).toJSON());
        }
        } catch (Exception e) {
//            e.printStackTrace();
        }
        return result;
    }
    /**
     * 解析普通的json
     * @param fileName
     * @param item
     * @param itemMap
     * @param idMap 子表需添加的id
     */
    public static void parseNormalItem(String fileName, Item item , Map<String,Item> itemMap, Map<String,String> idMap){
        Map<String, Object> eles = item.asMap();
        //主id的名字和值
        Map.Entry<String, String> pidEntry=null;
        if(idMap.size()>0) {
            pidEntry = idMap.entrySet().iterator().next();
        }
        for (String k : eles.keySet()){
            String newName = fileName+"_"+k;
            if(item.getTypeOf(k).getSimpleName().equals("ArrayList")){
                Item listItem = new Item();
                ArrayList<Object> objs = (ArrayList<Object>) item.get(k);
                int i = 0 ;
                for (Object o  : objs) {
                    String element = "list_target"+i;
                    idMap.put(k+"_row",i+"");
                    itemMap.put(newName,new Item());//添加临时item
                    if (o instanceof String){
                        Item strIt = new Item().withString("string_field",(String)o);
                        //添加主键
                        for (Map.Entry<String,String> identry : idMap.entrySet()) {
                            strIt.withString(identry.getKey(),identry.getValue());
                        }
                        parseNormalItem(newName,strIt,itemMap,idMap);
                    }
                    if (o instanceof Number){
                        Item numIt = new Item().withNumber("number_field", (Number) o);
                        //添加主键
                        for (Map.Entry<String,String> identry : idMap.entrySet()) {
                            numIt.withString(identry.getKey(),identry.getValue());
                        }
                        parseNormalItem(newName,numIt,itemMap,idMap);
                        parseNormalItem(newName,new Item().withNumber("number_field",(Number) o),itemMap,idMap);
                    }
                    if (o instanceof Map){
                        Item mapIt = Item.fromMap((Map<String, Object>) o);
                        for (Map.Entry<String,String> identry : idMap.entrySet()) {
                            mapIt.withString(identry.getKey(),identry.getValue());
                        }
                        parseNormalItem(newName,mapIt,itemMap,idMap);
                    }
                    if (o instanceof List){
                        //不合理
//                        parseItem(newName,new Item().withList("list_field",(List)o),itemMap,idMap,true);
                    }
                    listItem.with(element,itemMap.get(newName).asMap());
                    itemMap.remove(newName);//删除临时item
                    i++;
                }
                idMap.clear();
                if(pidEntry!=null){
                    idMap.put(pidEntry.getKey(),pidEntry.getValue());
                }
                if(listItem.numberOfAttributes()>0) {
                    itemMap.put(newName, listItem);//把list类型的数据添加
                }
            }
            if(item.getTypeOf(k).getSimpleName().equals("LinkedHashMap")){
                Item it = Item.fromMap((Map<String, Object>) item.get(k));
                //添加主键
                for (Map.Entry<String,String> identry : idMap.entrySet()) {
                    it.withString(identry.getKey(),identry.getValue());
                }
                itemMap.put(newName,new Item());
                parseNormalItem(newName,it,itemMap,idMap);
            }
            if(item.getTypeOf(k).getSimpleName().equals("BigDecimal")){
                itemMap.get(fileName).withNumber(k,item.getNumber(k));
            }
            if(item.getTypeOf(k).getSimpleName().equals("String")){
                itemMap.get(fileName).withString(k,item.getString(k));
            }
        }
    }


    /**
     * 获取32位uuid
     * @return
     */
    private static String getUUid() {
        UUID uuid = UUID.randomUUID();
        String guidStr = uuid.toString();
        md.update(guidStr.getBytes(), 0, guidStr.length());
        return new BigInteger(1, md.digest()).toString(16);
    }

}
