package com.patsnap.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MRTest2 extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        job.setNumReduceTasks(0);

        Path inpath = new Path(args[0]);
        Path outpaht = new Path(args[1]);
        FileInputFormat.addInputPath(job, inpath);
        FileOutputFormat.setOutputPath(job, outpaht);

        //map
        job.setMapperClass(ParseJsonMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //reduce
//        job.setReducerClass(ParseJsonReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
        // Defines additional single text based output 'text' for the job

        MultipleOutputs.addNamedOutput(job, "trademark", TextOutputFormat.class,
                Text.class, Text.class);

        boolean isSuccess = job.waitForCompletion(true);

        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration  configuration = new Configuration();
        int status = ToolRunner.run(configuration, new MRTest2(), args);

        System.exit(status);
    }
}
