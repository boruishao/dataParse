package com.patsnap.json;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.patsnap.utils.ParseRef;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class ParseJsonMR extends Configured implements Tool {
    private static final String LIST_TARGET = "list_target";
    private static String mainTName;
    private static String idName;

    public static String getMainTName() {
        return mainTName;
    }

    public static void setMainTName(String mainTName) {
        ParseJsonMR.mainTName = mainTName;
    }

    public static String getIdName() {
        return idName;
    }

    public static void setIdName(String idName) {
        ParseJsonMR.idName = idName;
    }

    /**
     * step 1: Mapper Class
     */
    public static class DyjsonMapper extends Mapper<LongWritable, Text,Text,Text>{
        private Text tableName = new Text();
        private Text record = new Text();
        private boolean caseSensitive;
        private MultipleOutputs<Text,Text> mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
            mos = new MultipleOutputs<Text,Text>(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = (caseSensitive) ?
                    value.toString() : value.toString().toLowerCase();
            Map<String, String> normalJsonByItem = ParseRef.getNormalJsonByItem(line, "trademark", "trademark_id");

            Counter counter = context.getCounter("com.patsnap", "successNum");
            if (normalJsonByItem.size()>0){
                counter.increment(1);
            }

            for (String tName : normalJsonByItem.keySet()) {
                String rec = normalJsonByItem.get(tName);
                if (rec.contains(LIST_TARGET)){
                    Map<String, Object> map = new Item().fromJSON(rec).asMap();
                    Collection<Object> values = map.values();
                    for (Object o  : values) {
                        if (o != null && o instanceof Map){
                            String json = new Item().fromMap((Map<String, Object>) o).toJSON();
                            tableName.set(tName);
                            record.set(json);
                            mos.write("test",tableName,record,tName);
                        }
                    }
                }else {
                    if (rec != null){
                        tableName.set(tName);
                        record.set(rec);
                        mos.write("test",tableName,record,tName);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }


    /**
     * step 2: Reduce Class
     */
    public static class DyjsonReader extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        // 1: get Configuration
        // Configuration configuration = new Configuration();
        Configuration conf = getConf() ;

        // 2: create job
        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        // 3: set job
        // input  -> map  -> reduce -> output
        // 3.1: input
        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);

        // 3.2: mapper
        job.setMapperClass(ParseJsonMR.DyjsonMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 3.3: reducer

        // 3.4: output
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);

        // 4: submit job
        boolean isSuccess = job.waitForCompletion(true) ;

        return isSuccess ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {

//         args = new String[]{ //
//         "hdfs://com.patsnap.user-1:8020/user/beifeng/input" ,//
//         "hdfs://com.patsnap.user-1 :8020/user/beifeng/output5"
//         };
        Configuration configuration = new Configuration() ;
        // set compress
//        configuration.set("mapreduce.map.output.compress", "true");
//        configuration.set("mapreduce.map.output.compress.codec", "xxxx");

        // run job
        //  public static int run(Configuration conf, Tool tool, String[] args)
        if (args.length>3){
            setMainTName(args[2]);
            setMainTName(args[3]);
        }

        int status = ToolRunner.run( //
                configuration,//
                new ParseJsonMR(), //
                args //
        );

        System.exit(status);
    }
}
