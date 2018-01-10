package com.patsnap.mapreduce;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.patsnap.utils.ParseRef;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class ParseJsonMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text tName = new Text();
    private Text record = new Text();
    private MultipleOutputs<Text, Text> mos;
    private static final String LIST_TARGET = "list_target";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        mos = new MultipleOutputs<Text, Text>(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Counter successCounter = context.getCounter("com.patsnap", "successNum");
        Counter failounter = context.getCounter("com.patsnap", "failNum");
        Map<String, String> normalJsons = ParseRef
                .getNormalJsonByItem(value.toString(), "trademark", "trademark_id");
        if (normalJsons.size() > 0) {
            successCounter.increment(1);
            for (Map.Entry<String, String> entry : normalJsons.entrySet()) {
                if (entry.getValue().contains(LIST_TARGET)) {
                    Map<String, Object> map = new Item().fromJSON(entry.getValue()).asMap();
                    Collection<Object> values = map.values();
                    for (Object o : values) {
                        if (o != null && o instanceof Map) {
                            String json = new Item().fromMap((Map<String, Object>) o).toJSON();
                            tName.set(entry.getKey());
                            record.set(json);
                            mos.write("trademark",tName, record,entry.getKey()+"/");
//                            context.write(tName, record);
                        }
                    }
                } else {
                    tName.set(entry.getKey());
                    record.set(entry.getValue());
                    mos.write("trademark",tName, record,entry.getKey()+"/");
                }
            }
        } else {
            failounter.increment(1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }


}
