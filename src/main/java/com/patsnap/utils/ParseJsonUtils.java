package com.patsnap.utils;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseJsonUtils {

    private static MessageDigest md;

    static {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过正则表达式解析json
     * @param str
     * @return
     */
    public static String horizontalParseJson(String str){
        return  str.substring(32, str.length() - 4) //多截取一位，并在最后拼上
                .replaceAll("\\{\"s\":|\\{\"n\":|\\{\"m\":|\\{\"l\":", "")
                .replaceAll("]}", "]")
                .replaceAll("\"}", "\"")
                .replaceAll("}}", "}") + "}";
    }

    /**
     * 通过递归解析json
     * @param str
     * @return
     */
    public static String longitudinalParseJson(String str){
        JSONObject jsonObject = JSONObject.fromObject(str.substring(32, str.length() - 3));
        StringBuffer sb = new StringBuffer();
        sb.append("{");
        parseJson(jsonObject, sb);
        if (sb.length() != 0) {
            if (sb.lastIndexOf("]") != sb.length()- 1) {
                sb.setLength(sb.length() - 1);
            }
        }
        sb.append('}');
        return  sb.toString();
    }

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
    private static String rowid = ""; //暂用静态变量，可能会存在并发问题
    private static void parseJson(JSONObject json, StringBuffer sb) {
        Set keySet = json.keySet();

        for (Object key : keySet) {
            String k = (String)key;
            String s = json.getString(k);

            if ("l".equals(k)) {
                sb.append("[");
                JSONArray fromObject = JSONArray.fromObject(s);
                for (Object object : fromObject) {
                    JSONObject json2 = JSONObject.fromObject(object);
                    parseJson(json2, sb);
                    sb.setLength(sb.length() - 1);
                    sb.append(",");
                }
                //如果list是空的，就会以[开头，则不需要把逗号去掉
                if (sb.lastIndexOf("[") != sb.length()- 1) {
                    sb.setLength(sb.length() - 1);
                }
                sb.append("]").append(",");
            }

            if ("m".equals(k)) {
                sb.append("{");
                JSONObject json2 = JSONObject.fromObject(s);
                String uuid = "{\"s\":\""+getUUid()+"\"}";  //如果是map需要生成符合格式的uuid
                json2.put("uuid",uuid);
                json2.put("rowid",rowid);
                parseJson(json2, sb);
                sb.setLength(sb.length() - 1);
                sb.append("}").append(",");
            }

            if ("n".equals(k)) {
                sb.append(s).append(",");
            }
            if ("s".equals(k)) {
                sb.append("\"").append(s).append("\"").append(",");
            }
            //拼接key
            if (k.length() > 1) {
                if ("country".equals(k)){
                    rowid= "{\"s\""+s.substring(4,s.length()-1)+"}";
                }
                sb.append("\"").append(key).append("\"").append(":");
                JSONObject json2 = json.getJSONObject(k);
                parseJson(json2, sb);
            }
        }
    }


    /**
     * 通过json获取schema不断迭代更新到hashSet中
     * hashSet 适合增删
     * linkedHashSet 适合查询
     * treeSet 有序
     * @param set
     * @param json
     */
    public static void getSchema(Set<String> set ,JSONObject json) {
        set.addAll(json.keySet());
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

    /**
     * 取出list中的部分
     * @param listName
     * @param json
     * @return
     */
    public static String getList(String listName,String json){
        String str ="";
        String regex = "\""+listName+"\":\\[.*?]";  //"\"[^,]*?\\[.*?]" 这个是匹配通用的
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(json);
        if(matcher.find()){
            String group = matcher.group(0);
            group = group.substring(0,group.length()-1);
            str = group.replaceAll("\".*?\":\\[", "");
        }
        return str;
    }

    /**
     * 遍历出list中的map
     * @param string
     * @return
     */
    public static String[] getMapInList(String string){
         List<String> list = new ArrayList<>();
        String regex = "\\{.*?}";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(string);
        while (matcher.find()) {
            list.add(matcher.group(0));
        }
        String[] strings = list.toArray(new String[list.size()]);
        return strings;
    }

    /**
     * 获取不在list中的map
     * @param mapName
     * @param json
     * @return
     */
    public static String getMap(String mapName,String json){
        String str = "";
        String regex = "\""+mapName+"\":\\{.*?}";

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(json);
        if(matcher.find()){
            String group = matcher.group(0);
            str = group.replaceAll("\".*?\":\\{", "{");
        }
        return str;
    }

    /**
     * 将json中的list对象转为uuid的数组
     * @param json
     * @return
     */
    public static String list2uuid(String json){
        String regex = "\"[^,{]*?\\[.*?]";
        JSONObject jsonobject = JSONObject.fromObject(json);
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(json);
        while (matcher.find()) {
            String group = matcher.group(0);
            group = group.substring(0,group.length());
            String s = group.substring(group.indexOf(":")+1);
            String key = group.substring(1,group.indexOf("\"",2));
            JSONArray jsonArray = JSONArray.fromObject(s);
            JSONArray newArray = new JSONArray();
            for (Object jsonobj : jsonArray) {
                JSONObject jsonObject = JSONObject.fromObject(jsonobj);
                Object uuid = jsonObject.get("uuid");
                newArray.add(uuid);
            }
            jsonobject.put(key,newArray);
        }
        return jsonobject.toString();
    }

    /**
     * 把指定的map，替换成uuid
     * @param mapName
     * @param json
     * @return
     */
    public static String map2uuid(String mapName,String json){
        JSONObject jsonObject = JSONObject.fromObject(json);
        Object trademark_source_id = jsonObject.get(mapName);
        JSONObject jsonObject1 = JSONObject.fromObject(trademark_source_id);
        jsonObject.put(mapName,jsonObject1.get("uuid"));
        return jsonObject.toString();
    }

    /**
     * 通用匹配map，但是嵌套的map不行
     * @param json
     * @return
     */
    public static String map2uuid(String json){
        String regex = "\"[^,\\[]*?\\{.*?}";
        JSONObject jsonObject = JSONObject.fromObject(json);
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(json);
        while (matcher.find()) {
            String group = matcher.group(0);
            String key = group.substring(1,group.indexOf("\"",2));
            String replace = group.replaceAll("\".*?\":\\{", "{");
            JSONObject jsonObject1 = JSONObject.fromObject(replace);
            jsonObject.put(key,jsonObject1.get("uuid"));
        }
        return jsonObject.toString();
    }

}